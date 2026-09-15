"""Parser for mcp.ai.example.test's own access log.

mcp.ai is not behind Nginx Proxy Manager (see WS-41.E2 part C): it sits behind a
LAN-bound nginx in front of LiteLLM's MCP gateway, and until now that nginx kept no
access log at all, so every request through it was invisible to this warehouse. A
parallel workstream in the platform repo gives it one -- JSON lines, one file per
UTC day, `access-YYYY-MM-DD.log` -- under this field contract:

    time                    ISO 8601, UTC
    remote_addr             the client address, as nginx saw the connection
    method
    host
    request_uri             path and query together, exactly like NPM's `$request_uri`
    status
    body_bytes_sent
    request_length          total request size, headers included -- no column takes it
    request_time            nginx's own latency figure -- no column takes it either
    upstream_status
    upstream_response_time  -- also has no column
    user_agent
    referer

This module turns one line of that log into the same `ParsedLine` shape
`parsing.npm_log` produces, so the rest of the pipeline -- redaction, geo, the
schema, the dbt models -- does not need to know this host's format is different
from NPM's three. Three of the contract's fields (`request_length`, `request_time`,
`upstream_response_time`) are read and then not kept: the existing raw schema has no
request-size or latency columns for the NPM lines either, and giving this host one
that primary and secondary rows leave null would make host_source change what a column
means rather than just where a row came from.
"""

from __future__ import annotations

import json
from datetime import datetime

from homelab_datalake.parsing.npm_log import ParsedLine, Rejected

#: What `assets.npm_access` records on every row parsed by this module, the same
#: way `proxy`/`standard`/`combined` name NPM's three.
LOG_FORMAT = "mcp_api_json"

# A line missing one of these could not have come from the platform's own logger --
# `time` and `status` are what the day filter and the schema both need to place a
# row at all, and `remote_addr` is null-safe (a null client already means "unknown"
# throughout this pipeline) but its *key* still has to exist to trust the rest of
# the line.
_REQUIRED = ("time", "status")


def parse_line(line: str) -> ParsedLine | Rejected:
    """Parse one line of mcp.ai's JSON access log. Never raises, like npm_log.parse_line."""
    raw = line.rstrip("\n")
    if not raw.strip():
        return Rejected(raw=raw, reason="blank")

    try:
        record = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return Rejected(raw=raw[:2000], reason="not valid JSON")
    if not isinstance(record, dict):
        return Rejected(raw=raw[:2000], reason="JSON line is not an object")

    missing = [name for name in _REQUIRED if name not in record]
    if missing:
        return Rejected(raw=raw[:2000], reason=f"missing field(s): {', '.join(missing)}")

    ts = _parse_time(record.get("time"))
    if ts is None:
        return Rejected(raw=raw[:2000], reason="unparseable timestamp")

    status = _int_or_none(record.get("status"))
    if status is None:
        return Rejected(raw=raw[:2000], reason="status is not an integer")

    return ParsedLine(
        log_format=LOG_FORMAT,
        timestamp=ts,
        client=_none_if_blank(record.get("remote_addr")),
        host=_none_if_blank(record.get("host")),
        method=_none_if_blank(record.get("method")),
        # Not in the contract. Left null for the same reason the `combined` NPM
        # format leaves it null: nothing here should invent a fact the source did
        # not log.
        scheme=None,
        uri=_none_if_blank(record.get("request_uri")),
        status=status,
        bytes_sent=_int_or_none(record.get("body_bytes_sent")),
        user_agent=_none_if_blank(record.get("user_agent")),
        referer=_none_if_blank(record.get("referer")),
        upstream_status=_none_if_blank(record.get("upstream_status")),
        cache_status=None,  # the gateway has nothing in front of it that caches
        sent_to=None,       # one upstream; nothing here to name
        gzip_ratio=None,    # not in the contract
    )


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        return None
    # The contract promises UTC; a naive value did not come from this logger and is
    # not safe to assume a zone for.
    return ts if ts.tzinfo is not None else None


def _int_or_none(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _none_if_blank(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value or None
