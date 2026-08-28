"""Extracting one day of LLM calls from LiteLLM's spend ledger.

Simpler than the log sources -- it is already a table, already typed, already keyed by
time. The work here is entirely about what *not* to take.

`LiteLLM_SpendLogs` carries three columns that must never reach the warehouse:
`messages`, `response` and `proxy_server_request` hold the full prompt and completion
payloads. They are empty on this deployment today because payload logging is off, and
that is exactly why they are excluded by name rather than by "select the ones that
happen to be empty": switching payload logging on must not silently start ingesting
everybody's conversations.

Two more are pseudonymised rather than dropped, because their grouping value is real:

  * `requester_ip_address` -- personal data, through the same boundary as the proxy
    logs (homelab_datalake.privacy).
  * `api_key` -- a mix of 64-character hashes and 24-character literals in this
    deployment. Nothing in the column says which is which, so it is treated as a
    credential and hashed. "Spend per key" survives; the risk does not.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.privacy import hash_client, redact_client

# Named explicitly, and never `select *`. A new column added upstream should arrive as
# a deliberate change here rather than appear in the warehouse on its own -- which is
# also how a prompt column would arrive.
SELECT_COLUMNS = (
    "request_id", "call_type", "api_key", "spend",
    "total_tokens", "prompt_tokens", "completion_tokens",
    '"startTime"', '"endTime"', '"completionStartTime"',
    "model", "model_group", "custom_llm_provider",
    "cache_hit", "team_id", "end_user", "status",
    "request_duration_ms", "session_id", "mcp_namespaced_tool_name",
    "requester_ip_address",
)

def _build_query() -> str:
    """The SELECT, assembled from the constant column list.

    Built in a function rather than as a module-level f-string so the interpolation
    sits on one line and can carry its own justification: SELECT_COLUMNS is a module
    constant and never input, and the only values reaching the database are bound
    parameters (%(start)s / %(end)s), which psycopg escapes.
    """
    columns = ", ".join(SELECT_COLUMNS)
    return (
        # The interpolation below is suppressed deliberately: it inserts constant
        # identifiers only, and the sole values reaching the database are bound
        # parameters, which psycopg escapes.
        f'SELECT {columns} FROM "LiteLLM_SpendLogs" '  # noqa: S608
        'WHERE "startTime" >= %(start)s AND "startTime" < %(end)s '
        'ORDER BY "startTime", request_id'
    )


QUERY = _build_query()


@dataclass
class SpendExtract:
    rows: list[dict] = field(default_factory=list)
    total_spend: float = 0.0
    calls: int = 0
    failures: int = 0
    models: set[str] = field(default_factory=set)


def connection_settings() -> dict:
    return {
        "host": os.environ.get("LITELLM_PG_HOST", "localhost"),
        "port": int(os.environ.get("LITELLM_PG_PORT", "5432")),
        "dbname": os.environ.get("LITELLM_PG_DB", "litellm"),
        "user": os.environ.get("LITELLM_PG_USER", ""),
        "password": os.environ.get("LITELLM_PG_PASSWORD", ""),
    }


def _naive_utc(value: datetime | None) -> datetime | None:
    """LiteLLM stores `timestamp without time zone`, already in UTC.

    Attaching UTC explicitly rather than leaving it naive means everything downstream
    compares against the proxy-log timestamps on the same footing, and the partition
    filter cannot drift with the session timezone. That bug already cost a day once --
    see dbt/tests/assert_partition_date_matches_timestamp.sql.
    """
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def extract_day(day: date, salt: str, geo: GeoEnricher | None = None, rows=None) -> SpendExtract:
    """One day of calls, redacted. `rows` is injectable so tests need no database."""
    geo = geo or GeoEnricher.from_env()
    start = datetime.combine(day, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)

    if rows is None:
        import psycopg

        with psycopg.connect(**connection_settings()) as conn, conn.cursor() as cur:
            cur.execute(QUERY, {"start": start.replace(tzinfo=None), "end": end.replace(tzinfo=None)})
            columns = [c.name for c in cur.description]
            rows = [dict(zip(columns, record, strict=True)) for record in cur.fetchall()]

    out = SpendExtract()
    for row in rows:
        started = _naive_utc(row.get("startTime"))
        if started is None or not (start <= started < end):
            continue

        address = (row.get("requester_ip_address") or "").strip()
        redacted = redact_client(address or None, salt)
        info = geo.lookup(address or None)

        key = (row.get("api_key") or "").strip()
        spend = float(row.get("spend") or 0.0)
        status = row.get("status") or None

        out.rows.append(
            {
                "dt": day,
                "request_id": row.get("request_id"),
                "session_id": row.get("session_id") or None,
                "started_at": started,
                "ended_at": _naive_utc(row.get("endTime")),
                "first_token_at": _naive_utc(row.get("completionStartTime")),
                "call_type": row.get("call_type") or None,
                "model": row.get("model") or None,
                "model_group": row.get("model_group") or None,
                "provider": row.get("custom_llm_provider") or None,
                "mcp_tool": row.get("mcp_namespaced_tool_name") or None,
                "status": status,
                "spend_usd": spend,
                "total_tokens": int(row.get("total_tokens") or 0),
                "prompt_tokens": int(row.get("prompt_tokens") or 0),
                "completion_tokens": int(row.get("completion_tokens") or 0),
                "duration_ms": row.get("request_duration_ms"),
                "cache_hit": (row.get("cache_hit") or "").strip() or None,
                "team_id": row.get("team_id") or None,
                "end_user": hash_client(row["end_user"], salt) if row.get("end_user") else None,
                "api_key_hash": hash_client(key, salt) if key else None,
                "client_hash": redacted.client_hash,
                "client_network": redacted.client_network,
                "client_trust": redacted.client_trust,
                "geo_country_code": info.country_code,
                "geo_asn": info.asn,
                "geo_as_org": info.as_org,
            }
        )
        out.calls += 1
        out.total_spend += spend
        if status == "failure":
            out.failures += 1
        if row.get("model"):
            out.models.add(row["model"])

    return out
