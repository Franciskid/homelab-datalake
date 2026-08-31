"""LiteLLM spend assets.

Partitioned by date only, unlike the proxy logs: there is one ledger, on one host, so
a host dimension would be a column with a single value pretending to be a partition.

The check that matters here is not row count but *cost*. A model priced wrongly, or a
runaway agent, shows up as spend per call jumping while everything else looks healthy.
"""

import os
from datetime import date

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

from homelab_datalake import layout
from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.litellm_spend import extract_day
from homelab_datalake.resources import LakeStorage

# The ledger's first row. Unlike the proxy logs there is no rotation, so history only
# grows -- but backfilling before this date would produce empty partitions rather than
# an error, which is the more confusing failure.
SPEND_START = "2026-06-16"

spend_partitions = DailyPartitionsDefinition(start_date=SPEND_START)

SPEND_PREFIX = layout.prefix("litellm_spend")

# A day whose average cost per call is far above trailing normal is the signal worth
# stopping on: a mispriced model, a retry storm, or an agent that will not terminate.
MAX_COST_PER_CALL_USD = 0.05

_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("request_id", pa.string()),
        ("session_id", pa.string()),
        ("started_at", pa.timestamp("us", tz="UTC")),
        ("ended_at", pa.timestamp("us", tz="UTC")),
        ("first_token_at", pa.timestamp("us", tz="UTC")),
        ("call_type", pa.string()),
        ("model", pa.string()),
        ("model_group", pa.string()),
        ("provider", pa.string()),
        ("mcp_tool", pa.string()),
        ("status", pa.string()),
        ("spend_usd", pa.float64()),
        ("total_tokens", pa.int64()),
        ("prompt_tokens", pa.int64()),
        ("completion_tokens", pa.int64()),
        ("duration_ms", pa.int64()),
        ("cache_hit", pa.string()),
        ("team_id", pa.string()),
        ("end_user", pa.string()),
        ("api_key_hash", pa.string()),
        ("client_hash", pa.string()),
        ("client_network", pa.string()),
        ("client_trust", pa.string()),
        ("geo_country_code", pa.string()),
        ("geo_asn", pa.int64()),
        ("geo_as_org", pa.string()),
        ("model_id", pa.string()),
        ("api_base", pa.string()),
        ("cache_key", pa.string()),
        ("organization_id", pa.string()),
        ("agent_id", pa.string()),
        ("user_hash", pa.string()),
        ("request_tags", pa.list_(pa.string())),
        # The token detail `total_tokens` cannot express. Nullable on purpose: zero
        # reasoning tokens and "the provider did not say" are different facts.
        ("cached_tokens", pa.int64()),
        ("prompt_audio_tokens", pa.int64()),
        ("reasoning_tokens", pa.int64()),
        ("completion_audio_tokens", pa.int64()),
        ("accepted_prediction_tokens", pa.int64()),
        ("rejected_prediction_tokens", pa.int64()),
        # The bill, split. One `spend_usd` could not answer "what did the prompt cost".
        ("input_cost_usd", pa.float64()),
        ("output_cost_usd", pa.float64()),
        ("tool_usage_cost_usd", pa.float64()),
        ("original_cost_usd", pa.float64()),
        ("discount_amount_usd", pa.float64()),
        ("attempted_retries", pa.int64()),
        ("max_retries", pa.int64()),
        ("litellm_overhead_ms", pa.float64()),
        ("applied_guardrails", pa.list_(pa.string())),
        ("api_key_alias", pa.string()),
        ("team_alias", pa.string()),
        ("model_map_key", pa.string()),
        ("error_information", pa.string()),
        # LiteLLM's metadata blob, whole. Kept so a figure nobody has asked for yet is
        # still on disk when they do.
        ("metadata_json", pa.string()),
        # The caller's address as the ledger recorded it, beside the hashed and geo
        # columns above. Internal container addresses on this deployment.
        ("requester_ip", pa.string()),
        # The prompt and completion payloads. Empty until LiteLLM's payload logging is
        # switched on; whole conversations afterwards.
        ("messages_json", pa.string()),
        ("response_json", pa.string()),
        ("proxy_server_request_json", pa.string()),
    ]
)

CHECK_COST_PER_CALL = "cost_per_call_within_threshold"


@asset(
    partitions_def=spend_partitions,
    group_name="litellm",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_COST_PER_CALL,
            asset="litellm_spend_raw",
            description=(
                "Fails when a day's average cost per call exceeds the threshold. Row "
                "counts stay healthy through a mispricing or a retry storm; the bill "
                "does not."
            ),
        )
    ],
    description=(
        "One day of LLM calls from LiteLLM's spend ledger: model, tokens, latency and "
        "cost. Prompts and completions are never selected; the caller address, API key "
        "and end user are hashed at the boundary."
    ),
)
def litellm_spend_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)

    salt = os.environ.get("CLIENT_IP_HASH_SALT", "")
    if not salt:
        raise ValueError(
            "CLIENT_IP_HASH_SALT is not set. Refusing to run rather than write "
            "reversible hashes of caller addresses and API keys."
        )

    result = extract_day(day, salt, GeoEnricher.from_env())
    context.log.info(
        "dt=%s %d calls, $%.4f, %d failures, %d models",
        day, result.calls, result.total_spend, result.failures, len(result.models),
    )

    prefix = f"{SPEND_PREFIX}/dt={day.isoformat()}"
    lake.delete_prefix(prefix)
    written = lake.write_table(f"{prefix}/data.parquet", pa.Table.from_pylist(result.rows, schema=_SCHEMA))

    cost_per_call = result.total_spend / result.calls if result.calls else 0.0

    yield Output(
        {"calls": result.calls, "spend": result.total_spend, "cost_per_call": cost_per_call},
        metadata={
            "calls": result.calls,
            "spend_usd": MetadataValue.float(round(result.total_spend, 4)),
            "cost_per_call_usd": MetadataValue.float(round(cost_per_call, 6)),
            "failures": result.failures,
            "distinct_models": len(result.models),
            "bytes_written": written,
            "destination": MetadataValue.path(f"{lake.describe()}/{prefix}/data.parquet"),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_COST_PER_CALL,
        passed=cost_per_call <= MAX_COST_PER_CALL_USD,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "cost_per_call_usd": MetadataValue.float(round(cost_per_call, 6)),
            "threshold_usd": MAX_COST_PER_CALL_USD,
            "calls": result.calls,
            "spend_usd": MetadataValue.float(round(result.total_spend, 4)),
        },
    )
