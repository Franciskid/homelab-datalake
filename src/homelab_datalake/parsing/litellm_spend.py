"""Extracting one day of LLM calls from LiteLLM's spend ledger.

Simpler than the log sources -- it is already a table, already typed, already keyed by
time. The work here is entirely about what *not* to take.

`messages`, `response` and `proxy_server_request` hold the full prompt and completion
payloads, and they are ingested: the lake's owner asked for the whole ledger rather
than a summary of it. Two consequences follow, and belong where somebody will read
them rather than in a commit message:

  * They are **empty today** -- 0 of [count withheld] rows -- because LiteLLM's payload logging
    is off. Selecting them changes nothing until it is switched on, at which point
    every conversation through the gateway lands in the lake on the next run. That is
    the intended behaviour here, not an accident of configuration.
  * Anything that can read the lake can then read those conversations: the SQL
    console, the workbench, and any agent granted the datalake tools.

`requester_ip_address` is landed raw *and* pseudonymised. On this deployment it is the
internal container address of whatever called the gateway, so the raw value is a fact
about one's own infrastructure rather than about a stranger -- unlike the proxy logs
next door, where the addresses belong to visitors and the hashing boundary stands.

One column is still pseudonymised, and the reason is narrow:

  * `api_key` -- a mix of 64-character hashes and 24-character literals in this
    deployment. Nothing in the column says which is which, so it is treated as a
    credential and hashed. A *working key* sitting in a table anybody with lake access
    can read is a different kind of problem from data being available, and hashing it
    costs nothing analytically: "spend per key" survives, and the key's human-readable
    alias travels in the clear beside it, which is what a person actually recognises.

`metadata` is landed whole. The extracted scalars below -- reasoning tokens, cached
tokens, the input-versus-output cost split, retries, guardrails -- are a convenience
over it rather than a filter on it.
"""

from __future__ import annotations

import json
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
    "model", "model_id", "model_group", "custom_llm_provider", "api_base",
    "cache_hit", "cache_key", "team_id", "end_user", "status",
    "request_duration_ms", "session_id", "mcp_namespaced_tool_name",
    "requester_ip_address",
    # Added because the lake had no answer for questions the ledger could answer:
    # `metadata` carries the reasoning and cached token counts and the input/output
    # cost split, and it is populated on every row. `request_tags` is populated on
    # [count withheld] of 36,637. `organization_id` and `agent_id` are empty on this deployment
    # today and are taken anyway -- a column that fills in later should not need a
    # code change to start being recorded.
    "metadata", "request_tags", "organization_id", "agent_id", '"user"',
    # The payloads. See the module docstring: empty today, ingested by decision, and
    # readable by anything that can read the lake once payload logging is switched on.
    "messages", "response", "proxy_server_request",
)

# Nothing inside `metadata` is held back. Kept as a named, empty constant rather than
# deleted outright, because "filter a key out of the blob" is a decision that could
# reasonably be taken again -- adding a name here is how, and a test asserts that
# whatever is listed really does not reach the row.
METADATA_NOT_STORED: frozenset[str] = frozenset()

def _as_int(value) -> int | None:
    """A JSON number that may be absent, null, or a string. Never a silent zero.

    Zero and "not reported" are different facts here: a model that does no reasoning
    reports zero reasoning tokens, and one whose provider does not report them at all
    is unknown. Averaging the second as the first understates every other model.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string_list(value) -> list[str]:
    """A JSON array as a list of strings, and anything else as empty."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item is not None]


def _as_json(value) -> str | None:
    """A nested object as JSON text, or None when there is nothing in it."""
    if value in (None, {}, [], ""):
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str, sort_keys=True)


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

        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        usage = metadata.get("usage_object") or {}
        prompt_details = usage.get("prompt_tokens_details") or {}
        completion_details = usage.get("completion_tokens_details") or {}
        costs = metadata.get("cost_breakdown") or {}
        model_map = metadata.get("model_map_information") or {}
        kept_metadata = {
            k: v for k, v in metadata.items() if k not in METADATA_NOT_STORED
        }

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
                "model_id": row.get("model_id") or None,
                "api_base": row.get("api_base") or None,
                "cache_key": row.get("cache_key") or None,
                "organization_id": row.get("organization_id") or None,
                "agent_id": row.get("agent_id") or None,
                # `user` is LiteLLM's own end-user label, distinct from `end_user`.
                # Hashed on the same boundary as every other caller identity.
                "user_hash": hash_client(row["user"], salt) if row.get("user") else None,
                "request_tags": _string_list(row.get("request_tags")),
                # The figures the lake had no column for. `total_tokens` alone cannot
                # tell a cached prompt from a fresh one, nor a reasoning model's hidden
                # work from its visible answer -- so a cost-per-token number computed
                # from it was wrong in both directions.
                "cached_tokens": _as_int(prompt_details.get("cached_tokens")),
                "prompt_audio_tokens": _as_int(prompt_details.get("audio_tokens")),
                "reasoning_tokens": _as_int(completion_details.get("reasoning_tokens")),
                "completion_audio_tokens": _as_int(completion_details.get("audio_tokens")),
                "accepted_prediction_tokens": _as_int(
                    completion_details.get("accepted_prediction_tokens")
                ),
                "rejected_prediction_tokens": _as_int(
                    completion_details.get("rejected_prediction_tokens")
                ),
                # One `spend_usd` could not be split into what the prompt cost and what
                # the answer cost, which is the first question asked of an LLM bill.
                "input_cost_usd": _as_float(costs.get("input_cost")),
                "output_cost_usd": _as_float(costs.get("output_cost")),
                "tool_usage_cost_usd": _as_float(costs.get("tool_usage_cost")),
                "original_cost_usd": _as_float(costs.get("original_cost")),
                "discount_amount_usd": _as_float(costs.get("discount_amount")),
                "attempted_retries": _as_int(metadata.get("attempted_retries")),
                "max_retries": _as_int(metadata.get("max_retries")),
                "litellm_overhead_ms": _as_float(metadata.get("litellm_overhead_time_ms")),
                "applied_guardrails": _string_list(metadata.get("applied_guardrails")),
                "api_key_alias": metadata.get("user_api_key_alias") or None,
                "team_alias": metadata.get("user_api_key_team_alias") or None,
                "model_map_key": model_map.get("model_map_key") or None,
                # Why a call failed, when one did. Kept as JSON: it is null on almost
                # every row and its shape is the provider's, not ours.
                "error_information": _as_json(metadata.get("error_information")),
                # Everything else the blob held, minus what METADATA_NOT_STORED keeps
                # out. Here so a figure nobody has asked for yet is still recorded.
                "metadata_json": _as_json(kept_metadata),
                # Raw beside the hash. These are internal container addresses on this
                # deployment -- a fact about one's own infrastructure. The hashed and
                # geo columns above stay, so nothing downstream has to change.
                "requester_ip": address or None,
                # The payloads, as JSON text. Null today; whole conversations once
                # LiteLLM's payload logging is switched on.
                "messages_json": _as_json(row.get("messages")),
                "response_json": _as_json(row.get("response")),
                "proxy_server_request_json": _as_json(row.get("proxy_server_request")),
            }
        )
        out.calls += 1
        out.total_spend += spend
        if status == "failure":
            out.failures += 1
        if row.get("model"):
            out.models.add(row["model"])

    return out
