"""Parser tests.

Every fixture line here is synthetic. Real log lines contain real visitor
addresses, and this repo does not publish data (PLAN.md section 5). The parser
was separately validated against [count withheld] real lines from both hosts with zero
rejects; these tests pin the behaviour that validation confirmed.
"""

from datetime import UTC, datetime

import pytest

from homelab_datalake.parsing.npm_log import (
    ParsedLine,
    Rejected,
    parse_line,
    parse_lines,
    parse_timestamp,
)

PROXY = (
    '[27/Aug/2026:20:42:55 +0000] - 200 200 - GET https tv.example.test '
    '"/AiSearch/Health" [Client 203.0.113.7] [Length 97] [Gzip -] '
    '[Sent-to 192.0.2.10] "Mozilla/5.0 (Macintosh)" "-"'
)
STANDARD = (
    '[24/Aug/2026:09:08:39 +0000] 400 - GET http 198.51.100.4 '
    '"/" [Client 198.51.100.4] [Length 252] [Gzip -] "Bot/1.0" "-"'
)
COMBINED = (
    '203.0.113.55 - - [24/Aug/2026:09:04:12 +0000] "GET /index.php HTTP/1.1" '
    '404 183 "-" "Mozilla/5.0 (Windows NT 10.0)"'
)


def test_proxy_format_fields():
    r = parse_line(PROXY)
    assert isinstance(r, ParsedLine)
    assert r.log_format == "proxy"
    assert r.timestamp == datetime(2026, 8, 27, 20, 42, 55, tzinfo=UTC)
    assert (r.client, r.host, r.method, r.scheme) == ("203.0.113.7", "tv.example.test", "GET", "https")
    assert (r.status, r.bytes_sent, r.sent_to) == (200, 97, "192.0.2.10")
    assert r.upstream_status == "200"
    assert r.cache_status is None  # "-" is absent, not the string "-"
    assert r.referer is None


def test_standard_format_has_no_upstream_fields():
    r = parse_line(STANDARD)
    assert isinstance(r, ParsedLine)
    assert r.log_format == "standard"
    assert r.status == 400
    assert r.upstream_status is None and r.sent_to is None


def test_combined_format_splits_the_request():
    r = parse_line(COMBINED)
    assert isinstance(r, ParsedLine)
    assert r.log_format == "combined"
    assert (r.method, r.uri, r.status) == ("GET", "/index.php", 404)
    assert r.host is None  # combined carries no Host header
    assert r.scheme is None


def test_upstream_status_can_hold_several_values_on_retry():
    """nginx writes "502, 200" when it retried. The 3-digit $status still follows."""
    line = (
        '[27/Aug/2026:20:42:55 +0000] - 502, 200 200 - GET https a.example.test '
        '"/" [Client 203.0.113.7] [Length 5] [Gzip -] [Sent-to 192.0.2.10] "UA" "-"'
    )
    r = parse_line(line)
    assert isinstance(r, ParsedLine)
    assert r.upstream_status == "502, 200"
    assert r.status == 200


def test_malformed_request_keeps_method_and_uri_null_rather_than_guessing():
    """TLS bytes sent to port 80. The line is real; the request line is not a request."""
    line = (
        r'203.0.113.9 - - [24/Aug/2026:09:04:12 +0000] "\x16\x03\x02\x00" 400 157 "-" "-"'
    )
    r = parse_line(line)
    assert isinstance(r, ParsedLine)
    assert r.log_format == "combined"
    assert r.method is None and r.uri is None
    assert r.status == 400


def test_ipv6_client_is_parsed():
    line = PROXY.replace("203.0.113.7", "2001:db8:1234:5678:9abc:def0:1234:5678")
    r = parse_line(line)
    assert isinstance(r, ParsedLine)
    assert r.client == "2001:db8:1234:5678:9abc:def0:1234:5678"


def test_unparseable_line_is_rejected_with_its_text_not_dropped():
    r = parse_line("this is not an access log line at all")
    assert isinstance(r, Rejected)
    assert r.reason == "no format matched"
    assert r.raw == "this is not an access log line at all"


def test_bad_timestamp_is_rejected_by_the_matching_format():
    line = PROXY.replace("27/Aug/2026", "99/Zzz/2026")
    assert isinstance(parse_line(line), Rejected)


@pytest.mark.parametrize("value", ["", "   ", "\n"])
def test_blank_lines_are_rejected_as_blank(value):
    r = parse_line(value)
    assert isinstance(r, Rejected) and r.reason == "blank"


def test_parse_timestamp_handles_non_utc_offsets():
    ts = parse_timestamp("27/Aug/2026:20:42:55 +0200")
    assert ts is not None
    assert ts.utcoffset().total_seconds() == 7200


def test_stats_count_every_line_and_split_by_format():
    lines = [PROXY, STANDARD, COMBINED, "garbage", ""]
    parsed, rejected, stats = parse_lines(lines)
    assert stats.total == 5
    assert stats.parsed == 3 and stats.rejected == 2
    assert stats.by_format == {"proxy": 1, "standard": 1, "combined": 1}
    assert len(parsed) == 3 and len(rejected) == 2
    assert stats.reject_rate == pytest.approx(0.4)


def test_the_referer_survives_into_the_parsed_line():
    """Parsed on every line since this was written, and dropped before the write.

    It is how a link somebody followed is told apart from a URL somebody typed, and
    the schema simply had no column for it.
    """
    line = (
        '[27/Aug/2026:20:42:55 +0000] MISS 200 200 - GET https data.example.test '
        '"/sql?tab=catalogue" [Client 10.0.0.5] [Length 5120] [Gzip 2.10] '
        '[Sent-to datalake-api] "Mozilla/5.0" "https://ai.example.test/workbench"'
    )
    parsed = parse_line(line)
    assert parsed.referer == "https://ai.example.test/workbench"
    assert parsed.gzip_ratio == "2.10"


def test_an_uncompressed_response_reports_no_ratio_rather_than_a_dash():
    line = (
        '[27/Aug/2026:20:42:55 +0000] MISS 200 200 - GET https data.example.test '
        '"/health" [Client 10.0.0.5] [Length 12] [Gzip -] '
        '[Sent-to datalake-api] "curl/8.0" "-"'
    )
    parsed = parse_line(line)
    assert parsed.gzip_ratio is None
    assert parsed.referer is None
