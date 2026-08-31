"""Parser for Nginx Proxy Manager access logs.

NPM writes three different line formats and does not say which is which. Two are
its own, declared in /etc/nginx/conf.d/include/log.conf:

    proxy     [$time_local] $upstream_cache_status $upstream_status $status -
              $request_method $scheme $host "$request_uri" [Client $remote_addr]
              [Length $body_bytes_sent] [Gzip $gzip_ratio] [Sent-to $server]
              "$http_user_agent" "$http_referer"

    standard  the same without the two upstream fields and without Sent-to

The third is stock nginx `combined`, which is what default-host_access.log gets.

Dispatch is by shape, most specific first. A line that matches nothing is not
dropped -- it is returned as a reject carrying its raw text, so the reject rate
can be tested against a threshold. Silently discarding lines you cannot parse is
how a pipeline lies about its own completeness.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# nginx writes month abbreviations in the C locale, always English. Mapping them
# explicitly is both faster than strptime and immune to the runner's locale.
_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

_TS = re.compile(
    r"^(?P<d>\d{2})/(?P<mon>[A-Za-z]{3})/(?P<y>\d{4}):"
    r"(?P<H>\d{2}):(?P<M>\d{2}):(?P<S>\d{2})\s"
    r"(?P<sign>[+-])(?P<oh>\d{2})(?P<om>\d{2})$"
)

# $upstream_status can hold several values on a retry ("502, 200"), so it is matched
# non-greedily up to the three-digit $status that always follows it.
_PROXY = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+"
    r"(?P<cache_status>\S+)\s+(?P<upstream_status>.+?)\s+(?P<status>\d{3})\s+-\s+"
    r"(?P<method>\S+)\s+(?P<scheme>\S+)\s+(?P<host>\S+)\s+"
    r'"(?P<uri>.*?)"\s+'
    r"\[Client (?P<client>[^\]]*)\]\s+\[Length (?P<length>[^\]]*)\]\s+"
    r"\[Gzip (?P<gzip>[^\]]*)\]\s+\[Sent-to (?P<sent_to>[^\]]*)\]\s+"
    r'"(?P<user_agent>.*?)"\s+"(?P<referer>.*?)"\s*$'
)

_STANDARD = re.compile(
    r"^\[(?P<ts>[^\]]+)\]\s+(?P<status>\d{3})\s+-\s+"
    r"(?P<method>\S+)\s+(?P<scheme>\S+)\s+(?P<host>\S+)\s+"
    r'"(?P<uri>.*?)"\s+'
    r"\[Client (?P<client>[^\]]*)\]\s+\[Length (?P<length>[^\]]*)\]\s+"
    r"\[Gzip (?P<gzip>[^\]]*)\]\s+"
    r'"(?P<user_agent>.*?)"\s+"(?P<referer>.*?)"\s*$'
)

_COMBINED = re.compile(
    r"^(?P<client>\S+)\s+\S+\s+(?P<remote_user>\S+)\s+"
    r"\[(?P<ts>[^\]]+)\]\s+"
    r'"(?P<request>.*?)"\s+(?P<status>\d{3})\s+(?P<length>\S+)\s+'
    r'"(?P<referer>.*?)"\s+"(?P<user_agent>.*?)"\s*$'
)

# "GET /path HTTP/1.1". Malformed requests (TLS bytes sent to port 80, mostly)
# will not match, and then method and uri stay null rather than being invented.
_REQUEST = re.compile(r"^(?P<method>[A-Za-z_]+)\s+(?P<uri>\S*)(?:\s+(?P<protocol>\S+))?$")


@dataclass
class ParsedLine:
    """One access-log line, still carrying its raw client address.

    The address is dropped by the privacy boundary before anything is written;
    see homelab_datalake.privacy. Nothing in this module hashes or persists.
    """

    log_format: str
    timestamp: datetime
    client: str | None
    host: str | None
    method: str | None
    scheme: str | None
    uri: str | None
    status: int | None
    bytes_sent: int | None
    user_agent: str | None
    referer: str | None
    upstream_status: str | None = None
    cache_status: str | None = None
    sent_to: str | None = None
    #: `$gzip_ratio`, which both NPM formats log and neither used to keep. "-" when
    #: the response was not compressed, which is a fact about the response.
    gzip_ratio: str | None = None


@dataclass
class Rejected:
    """A line no format matched. Kept, with its reason, so it can be counted."""

    raw: str
    reason: str


@dataclass
class ParseStats:
    total: int = 0
    parsed: int = 0
    rejected: int = 0
    by_format: dict[str, int] = field(default_factory=dict)

    @property
    def reject_rate(self) -> float:
        return self.rejected / self.total if self.total else 0.0


def parse_timestamp(value: str) -> datetime | None:
    """`27/Aug/2026:20:42:55 +0000` -> an aware datetime, or None if malformed."""
    m = _TS.match(value.strip())
    if not m:
        return None
    month = _MONTHS.get(m["mon"])
    if month is None:
        return None
    offset = timedelta(hours=int(m["oh"]), minutes=int(m["om"]))
    if m["sign"] == "-":
        offset = -offset
    try:
        return datetime(
            int(m["y"]), month, int(m["d"]),
            int(m["H"]), int(m["M"]), int(m["S"]),
            tzinfo=timezone(offset),
        )
    except ValueError:
        return None


def _int_or_none(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value or value == "-":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _dash_to_none(value: str | None) -> str | None:
    """nginx writes an absent value as `-`, which is not the same as an empty one."""
    if value is None:
        return None
    value = value.strip()
    return None if value in ("", "-") else value


def parse_line(line: str) -> ParsedLine | Rejected:
    """Parse one line. Never raises, never returns None -- a failure is a Rejected."""
    raw = line.rstrip("\n")
    if not raw.strip():
        return Rejected(raw=raw, reason="blank")

    for name, pattern in (("proxy", _PROXY), ("standard", _STANDARD)):
        m = pattern.match(raw)
        if not m:
            continue
        ts = parse_timestamp(m["ts"])
        if ts is None:
            return Rejected(raw=raw, reason=f"{name}: unparseable timestamp")
        groups = m.groupdict()
        return ParsedLine(
            log_format=name,
            timestamp=ts,
            client=_dash_to_none(groups["client"]),
            host=_dash_to_none(groups["host"]),
            method=_dash_to_none(groups["method"]),
            scheme=_dash_to_none(groups["scheme"]),
            uri=groups["uri"] or None,
            status=_int_or_none(groups["status"]),
            bytes_sent=_int_or_none(groups["length"]),
            user_agent=_dash_to_none(groups["user_agent"]),
            referer=_dash_to_none(groups["referer"]),
            upstream_status=_dash_to_none(groups.get("upstream_status")),
            cache_status=_dash_to_none(groups.get("cache_status")),
            sent_to=_dash_to_none(groups.get("sent_to")),
            gzip_ratio=_dash_to_none(groups.get("gzip")),
        )

    m = _COMBINED.match(raw)
    if m:
        ts = parse_timestamp(m["ts"])
        if ts is None:
            return Rejected(raw=raw, reason="combined: unparseable timestamp")
        method = uri = None
        req = _REQUEST.match(m["request"].strip())
        if req:
            method, uri = req["method"], req["uri"]
        return ParsedLine(
            log_format="combined",
            timestamp=ts,
            client=_dash_to_none(m["client"]),
            host=None,  # combined carries no Host header
            method=method,
            scheme=None,
            uri=uri,
            status=_int_or_none(m["status"]),
            bytes_sent=_int_or_none(m["length"]),
            user_agent=_dash_to_none(m["user_agent"]),
            referer=_dash_to_none(m["referer"]),
        )

    return Rejected(raw=raw, reason="no format matched")


def parse_lines(lines) -> tuple[list[ParsedLine], list[Rejected], ParseStats]:
    """Parse an iterable of lines, returning the good, the bad, and the counts."""
    parsed: list[ParsedLine] = []
    rejected: list[Rejected] = []
    stats = ParseStats()
    for line in lines:
        stats.total += 1
        result = parse_line(line)
        if isinstance(result, ParsedLine):
            parsed.append(result)
            stats.parsed += 1
            stats.by_format[result.log_format] = stats.by_format.get(result.log_format, 0) + 1
        else:
            rejected.append(result)
            stats.rejected += 1
    return parsed, rejected, stats
