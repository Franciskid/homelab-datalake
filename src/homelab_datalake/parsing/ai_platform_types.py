"""Postgres column types for the ai_platform registry, and the Arrow schema built from them.

Every selected column defaults to a string: `text` and `uuid` both land as strings
verbatim, and `jsonb` lands as JSON text (see `ai_platform.py`) -- so a string is the
right default for the overwhelming majority of the 700-odd selected columns across
the registry. `PG_TYPE_OVERRIDES` names only the columns whose real Postgres type
would otherwise be landed as the wrong Arrow type: `integer`, `numeric`,
`double precision`, `boolean` and `timestamp with time zone`. It says nothing about
the two `bytea` columns (`generated_files.data`, `message_attachments.data`) --
those never reach this schema at all, replaced by a derived `object_key` string, and
are looked up through `TableSpec.binary_column` instead.

Read off `ai_platform`'s live `information_schema.columns` on 2026-09-14, the same
day as the registry's column lists, and checked against it: every table in
`ai_platform_registry.REGISTRY` and every selected column that is not a plain string
appears here, in `tests/test_ai_platform_registry.py`.
"""

from __future__ import annotations

import pyarrow as pa

from homelab_datalake.parsing.ai_platform_registry import TableSpec

PG_TYPE_OVERRIDES: dict[str, dict[str, str]] = {
    "agent_versions": {"version": "integer", "created_at": "timestamp with time zone"},
    "agents": {"current_version": "integer", "created_at": "timestamp with time zone",
               "updated_at": "timestamp with time zone"},
    "api_keys": {"last_used_at": "timestamp with time zone", "revoked_at": "timestamp with time zone",
                 "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "application_owners": {"created_at": "timestamp with time zone"},
    "applications": {"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
                      "monthly_budget_eur": "numeric", "last_used_at": "timestamp with time zone"},
    "approvals": {"expires_at": "timestamp with time zone", "decided_at": "timestamp with time zone",
                  "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "audit_events": {"occurred_at": "timestamp with time zone"},
    "automation_runs": {"started_at": "timestamp with time zone", "finished_at": "timestamp with time zone",
                         "created_at": "timestamp with time zone"},
    "automation_step_runs": {"started_at": "timestamp with time zone",
                              "finished_at": "timestamp with time zone", "step_position": "integer"},
    "automation_steps": {"position": "integer", "created_at": "timestamp with time zone",
                          "updated_at": "timestamp with time zone"},
    "automations": {"enabled": "boolean", "created_at": "timestamp with time zone",
                     "updated_at": "timestamp with time zone", "consecutive_failures": "integer",
                     "paused_at": "timestamp with time zone"},
    "budgets": {"amount_eur": "numeric", "alert_threshold_percent": "integer", "hard_limit": "boolean",
                "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "conversations": {"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
                       "deleted_at": "timestamp with time zone"},
    "document_chunks": {"indexed_at": "timestamp with time zone"},
    "document_tags": {"curated": "boolean", "created_at": "timestamp with time zone",
                       "updated_at": "timestamp with time zone"},
    "email_recipients": {"archived_at": "timestamp with time zone", "created_at": "timestamp with time zone",
                          "updated_at": "timestamp with time zone"},
    "email_sends": {"template_version": "integer", "created_at": "timestamp with time zone"},
    "generated_file_tags": {"created_at": "timestamp with time zone"},
    "generated_files": {"byte_size": "integer", "created_at": "timestamp with time zone"},
    "incidents": {"occurrences": "integer", "opened_at": "timestamp with time zone",
                  "last_seen_at": "timestamp with time zone", "resolved_at": "timestamp with time zone"},
    "index_build_states": {"started_at": "timestamp with time zone",
                            "finished_at": "timestamp with time zone",
                            "last_successful_at": "timestamp with time zone", "indexed": "integer",
                            "skipped": "integer", "failed": "integer", "cost_eur": "numeric",
                            "embed_tokens": "integer", "updated_at": "timestamp with time zone"},
    "mcp_servers": {"tool_count": "integer", "auth_updated_at": "timestamp with time zone",
                     "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "media_items": {"year": "integer", "community_rating": "double precision",
                     "indexed_at": "timestamp with time zone"},
    "message_attachments": {"byte_size": "integer", "created_at": "timestamp with time zone"},
    "messages": {"created_at": "timestamp with time zone"},
    "model_routes": {"context_window_tokens": "integer", "input_cost_eur_per_1m": "numeric",
                      "output_cost_eur_per_1m": "numeric", "created_at": "timestamp with time zone",
                      "updated_at": "timestamp with time zone"},
    "notifications": {"read_at": "timestamp with time zone", "created_at": "timestamp with time zone"},
    "platform_settings": {"data_retention_days": "integer", "created_at": "timestamp with time zone",
                           "updated_at": "timestamp with time zone", "media_index_enabled": "boolean",
                           "media_index_cadence_hours": "integer",
                           "media_default_max_retrieve": "integer",
                           "media_include_watched_default": "boolean",
                           "documents_index_enabled": "boolean", "documents_chunk_size": "integer",
                           "documents_chunk_overlap": "integer"},
    "providers": {"credential_updated_at": "timestamp with time zone",
                  "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "push_subscriptions": {"created_at": "timestamp with time zone"},
    "report_alert_rules": {"threshold": "numeric", "enabled": "boolean",
                            "created_at": "timestamp with time zone",
                            "updated_at": "timestamp with time zone", "consecutive_runs": "integer",
                            "retired_at": "timestamp with time zone"},
    "report_alerts": {"value": "numeric", "threshold": "numeric", "created_at": "timestamp with time zone"},
    "report_definitions": {"comparison_enabled": "boolean", "notify_on_ready": "boolean",
                            "notify_on_failure": "boolean", "created_at": "timestamp with time zone",
                            "updated_at": "timestamp with time zone"},
    "reports": {"period_start": "timestamp with time zone", "period_end": "timestamp with time zone",
                "payload_version": "integer", "created_at": "timestamp with time zone",
                "dataset_version": "integer", "layout_version": "integer",
                "archived_at": "timestamp with time zone"},
    "resource_mappings": {"created_at": "timestamp with time zone",
                           "updated_at": "timestamp with time zone"},
    "run_steps": {"sequence": "integer", "occurred_at": "timestamp with time zone"},
    "runs": {"input_tokens": "integer", "output_tokens": "integer", "cost_eur": "numeric",
             "started_at": "timestamp with time zone", "completed_at": "timestamp with time zone",
             "awaiting_input_deadline": "timestamp with time zone", "reasoning_tokens": "integer"},
    "sandbox_executions": {"input_rows": "integer", "wall_time_ms": "integer",
                            "output_bytes": "integer", "created_at": "timestamp with time zone",
                            "completed_at": "timestamp with time zone"},
    "sandbox_policy": {"enabled": "boolean", "min_input_rows": "integer",
                        "max_execution_ms": "integer", "max_cpu_seconds": "integer",
                        "max_memory_mb": "integer", "max_output_bytes": "integer",
                        "max_code_bytes": "integer", "max_concurrent_runs": "integer",
                        "max_runs_per_hour": "integer", "created_at": "timestamp with time zone",
                        "updated_at": "timestamp with time zone"},
    "schedule_runs": {"ran_at": "timestamp with time zone", "created_at": "timestamp with time zone"},
    "schedules": {"hour": "integer", "minute": "integer", "weekday": "integer", "enabled": "boolean",
                  "next_run_at": "timestamp with time zone", "last_run_at": "timestamp with time zone",
                  "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone",
                  "consecutive_failures": "integer", "paused_at": "timestamp with time zone"},
    "script_versions": {"version": "integer", "created_at": "timestamp with time zone",
                         "verified_at": "timestamp with time zone"},
    "scripts": {"current_version": "integer", "archived_at": "timestamp with time zone",
                "created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "team_members": {"joined_at": "timestamp with time zone"},
    "teams": {"created_at": "timestamp with time zone", "updated_at": "timestamp with time zone"},
    "tool_definitions": {"created_at": "timestamp with time zone",
                          "updated_at": "timestamp with time zone"},
    "tool_grants": {"granted_at": "timestamp with time zone", "expires_at": "timestamp with time zone",
                     "revoked_at": "timestamp with time zone"},
    "usage_events": {"input_tokens": "integer", "output_tokens": "integer", "cost_eur": "numeric",
                      "latency_ms": "integer", "created_at": "timestamp with time zone"},
    "user_preferences": {"updated_at": "timestamp with time zone"},
    "user_roles": {"assigned_at": "timestamp with time zone"},
    "user_tool_configs": {"created_at": "timestamp with time zone",
                           "updated_at": "timestamp with time zone", "confirm_before_use": "boolean"},
    "users": {"first_seen_at": "timestamp with time zone", "last_seen_at": "timestamp with time zone",
              "last_login_at": "timestamp with time zone", "created_at": "timestamp with time zone",
              "updated_at": "timestamp with time zone"},
}

# Postgres type name -> Arrow type, for everything named in PG_TYPE_OVERRIDES.
# `integer` becomes int64, not int32: nothing in this warehouse's other sources uses
# int32 either (see litellm_spend.py's schema), and a second integer width bought
# nothing here but a reason to get it wrong once.
_PG_TO_ARROW: dict[str, pa.DataType] = {
    "integer": pa.int64(),
    "numeric": pa.float64(),
    "double precision": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp with time zone": pa.timestamp("us", tz="UTC"),
}


def arrow_type_for(table: str, column: str) -> pa.DataType:
    """The Arrow type one selected, non-binary column lands as.

    Defaults to string: `text` and `uuid` both land as strings verbatim, `jsonb`
    lands as JSON text (see `ai_platform.py`) -- everything not named in
    `PG_TYPE_OVERRIDES` is exactly one of those three.
    """
    pg_type = PG_TYPE_OVERRIDES.get(table, {}).get(column)
    return _PG_TO_ARROW.get(pg_type, pa.string()) if pg_type else pa.string()


def arrow_schema_for(spec: TableSpec) -> pa.Schema:
    """The full landed schema for one table: `dt` first, then every selected column
    in order (minus the binary one, if any), then the columns extraction adds.
    """
    fields = [("dt", pa.date32())]
    for column in spec.columns:
        if column == spec.binary_column:
            continue
        fields.append((column, arrow_type_for(spec.name, column)))

    if spec.binary_column:
        # The bytes never land; an object key pointing at them does.
        fields.append(("object_key", pa.string()))

    if spec.decode_metadata_images:
        # usage_events only. Ordered list of the objects the frames in this row's
        # metadata were decoded to, plus the two figures a check can verify against
        # without opening any of them -- see the frame-count asset check.
        fields.append(("frame_keys", pa.list_(pa.string())))
        fields.append(("frame_count", pa.int64()))
        fields.append(("frame_bytes", pa.int64()))

    return pa.schema(fields)
