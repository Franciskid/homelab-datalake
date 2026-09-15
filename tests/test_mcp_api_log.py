"""Parser tests for mcp.ai.example.test's own JSON access log.

Every fixture line here is synthetic, built from the field contract in WS-41.E2,
not copied from a real log -- there is no real one yet. Addresses are documentation
ranges (RFC 5737), same rule as tests/test_npm_log.py.
"""

import json
from datetime import UTC, datetime

import pytest

from homelab_datalake.parsing.mcp_api_log import LOG_FORMAT, parse_line
from homelab_datalake.parsing.npm_log import ParsedLine, Rejected

LINE = json.dumps(
    {
        "time": "2026-09-14T20:42:55Z",
        "remote_addr": "203.0.113.9",
        "method": "POST",
        "host": "mcp.ai.example.test",
        "request_uri": "/mcp/tools/call?tool=web_traffic",
        "status": 200,
        "body_bytes_sent": 512,
        "request_length": 843,
        "request_time": "0.041",
        "upstream_status": "200",
        "upstream_response_time": "0.038",
        "user_agent": "litellm/1.0",
        "referer": "-",
    }
)


def test_a_full_line_maps_onto_the_same_shape_npm_log_produces():
    r = parse_line(LINE)
    assert isinstance(r, ParsedLine)
    assert r.log_format == LOG_FORMAT
    assert r.timestamp == datetime(2026, 9, 14, 20, 42, 55, tzinfo=UTC)
    assert r.client == "203.0.113.9"
    assert r.host == "mcp.ai.example.test"
    assert r.method == "POST"
    assert r.uri == "/mcp/tools/call?tool=web_traffic"
    assert r.status == 200
    assert r.bytes_sent == 512
    assert r.upstream_status == "200"
    assert r.user_agent == "litellm/1.0"
    # "-" is nginx's absent marker; the contract may or may not send it literally,
    # and either way it must not read as a referer of "-".
    assert r.referer is None


def test_fields_the_contract_has_no_column_for_are_simply_not_carried():
    """request_length, request_time and upstream_response_time are read and dropped.

    Not an oversight: the existing raw schema has no request-size or latency
    columns for the NPM lines either (see the module docstring), so this host
    getting one would make host_source change what a column means.
    """
    r = parse_line(LINE)
    assert isinstance(r, ParsedLine)
    for absent in ("request_length", "request_time", "upstream_response_time"):
        assert not hasattr(r, absent)


def test_scheme_is_left_null_like_the_combined_npm_format():
    """Not in the contract; nothing here should invent a fact the source did not log."""
    r = parse_line(LINE)
    assert isinstance(r, ParsedLine)
    assert r.scheme is None


def test_cache_status_and_sent_to_are_null_for_this_format():
    r = parse_line(LINE)
    assert isinstance(r, ParsedLine)
    assert r.cache_status is None
    assert r.sent_to is None
    assert r.gzip_ratio is None


@pytest.mark.parametrize("missing", ["time", "status"])
def test_a_line_missing_a_required_field_is_rejected(missing):
    record = json.loads(LINE)
    del record[missing]
    r = parse_line(json.dumps(record))
    assert isinstance(r, Rejected)
    assert missing in r.reason


def test_a_null_remote_addr_is_an_unknown_client_not_a_reject():
    """The contract might legitimately have nothing here; null is an ordinary value
    throughout this pipeline, not evidence the line is malformed."""
    record = json.loads(LINE)
    record["remote_addr"] = None
    r = parse_line(json.dumps(record))
    assert isinstance(r, ParsedLine)
    assert r.client is None


def test_malformed_json_is_rejected_with_its_text_kept():
    r = parse_line("{not json at all")
    assert isinstance(r, Rejected)
    assert r.reason == "not valid JSON"
    assert r.raw == "{not json at all"


def test_a_json_array_is_rejected_as_not_an_object():
    r = parse_line("[1, 2, 3]")
    assert isinstance(r, Rejected)
    assert r.reason == "JSON line is not an object"


@pytest.mark.parametrize("value", ["", "   ", "\n"])
def test_blank_lines_are_rejected_as_blank(value):
    r = parse_line(value)
    assert isinstance(r, Rejected) and r.reason == "blank"


def test_a_naive_timestamp_is_rejected_rather_than_assumed_utc():
    """The contract promises UTC; a value with no offset did not come from this logger."""
    record = json.loads(LINE)
    record["time"] = "2026-09-14T20:42:55"
    r = parse_line(json.dumps(record))
    assert isinstance(r, Rejected)
    assert r.reason == "unparseable timestamp"


def test_an_unparseable_timestamp_is_rejected():
    record = json.loads(LINE)
    record["time"] = "not a timestamp"
    r = parse_line(json.dumps(record))
    assert isinstance(r, Rejected)
    assert r.reason == "unparseable timestamp"


def test_a_non_integer_status_is_rejected():
    record = json.loads(LINE)
    record["status"] = "not-a-number"
    r = parse_line(json.dumps(record))
    assert isinstance(r, Rejected)
    assert r.reason == "status is not an integer"


def test_a_status_sent_as_a_json_string_is_still_accepted():
    """nginx logging modules vary in whether numeric fields come out quoted."""
    record = json.loads(LINE)
    record["status"] = "200"
    r = parse_line(json.dumps(record))
    assert isinstance(r, ParsedLine)
    assert r.status == 200


def test_an_offset_other_than_z_is_accepted():
    record = json.loads(LINE)
    record["time"] = "2026-09-14T22:42:55+02:00"
    r = parse_line(json.dumps(record))
    assert isinstance(r, ParsedLine)
    assert r.timestamp == datetime(2026, 9, 14, 20, 42, 55, tzinfo=UTC)


# --- the platform's own validated sample line -----------------------------------
#
# ai-platform confirmed this exact shape against the deployed nginx config, with
# one substitution: `remote_addr` is a documentation address here, never the real
# one that line carried -- tests/test_no_real_addresses_in_fixtures.py forbids a
# real address in any tracked file, and is right to. Everything else -- the
# `+00:00` offset, upstream_status as a comma-separated retry list, an empty
# string referer, an integer status -- is exactly what was confirmed.
PLATFORM_SAMPLE = json.dumps(
    {
        "time": "2026-09-14T22:08:27+00:00",
        "remote_addr": "203.0.113.117",
        "method": "POST",
        "host": "127.0.0.1",
        "request_uri": "/mcp?foo=bar",
        "status": 502,
        "body_bytes_sent": 157,
        "request_length": 212,
        "request_time": 0.002,
        "upstream_status": "502, 502",
        "upstream_response_time": "0.003, 0.000",
        "user_agent": "test-agent/1.0",
        "referer": "",
    }
)


def test_the_platforms_own_sample_line_parses():
    r = parse_line(PLATFORM_SAMPLE)
    assert isinstance(r, ParsedLine)
    assert r.timestamp == datetime(2026, 9, 14, 22, 8, 27, tzinfo=UTC)
    assert r.client == "203.0.113.117"
    assert r.host == "127.0.0.1"
    assert r.method == "POST"
    assert r.uri == "/mcp?foo=bar"
    assert r.status == 502
    assert r.bytes_sent == 157
    assert r.user_agent == "test-agent/1.0"


def test_upstream_status_survives_a_comma_separated_retry_list():
    """nginx writes several statuses, comma-separated, when it retried upstreams --
    the same shape $upstream_status already has in the proxy NPM format (see
    test_upstream_status_can_hold_several_values_on_retry in test_npm_log.py).
    Nothing here parses it as a number, so the list passes through whole."""
    r = parse_line(PLATFORM_SAMPLE)
    assert isinstance(r, ParsedLine)
    assert r.upstream_status == "502, 502"


def test_an_empty_string_referer_is_treated_as_absent():
    """Distinct from the "-" case above: nginx's json_log module can write an
    unset variable as an empty string rather than the classic-log-format dash,
    and both must mean the same thing here."""
    r = parse_line(PLATFORM_SAMPLE)
    assert isinstance(r, ParsedLine)
    assert r.referer is None


def test_a_float_request_time_is_read_and_dropped_without_erroring():
    """request_time on the real sample is a JSON number (0.002), not a quoted
    string like the earlier fixtures use -- confirming _int_or_none's bool/str/int
    branches are not the only shapes this module has to tolerate for a field it
    never keeps anyway."""
    r = parse_line(PLATFORM_SAMPLE)
    assert isinstance(r, ParsedLine)
    assert not hasattr(r, "request_time")
