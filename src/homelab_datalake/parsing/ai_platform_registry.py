"""The declarative registry for the AI platform's database: what is landed, and how.

The owner's database `ai_platform` backs their self-hosted AI platform -- every
api.ai gateway call, every agent run and tool call, every conversation, every email
sent, every automation run. Measured on 2026-09-14 it held none of it: `usage_events`
alone carried [count withheld] calls over [volume withheld] and [count withheld] camera frames encoded as base64 in a
JSON column, and `run_steps` carried [count withheld] tool calls in the trailing week that never
passed through any gateway. The owner's decision, recorded here and in `PLAN.md` /
`IMPLEMENTATION_PLAN.md`, is that the warehouse holds all of it, queryable through
datasets -- reversing the earlier "prompts and responses are personal, not stored"
line.

One module lists every table once, rather than fifty near-identical asset functions
each re-deciding what to select. `homelab_datalake.assets.ai_platform` is a single
factory that builds one Dagster asset per `TableSpec` here; `homelab_datalake.layout`
registers one raw dataset per spec; `infra/ai-platform-datalake-role.sql` is rendered
from this file by `render_grant_sql()` below, so the database role can never grant
more than the registry selects. Add a table here and it is ingested, granted and
catalogued; leave it out and it is invisible everywhere at once.

## What every table is, in two shapes

`ai_platform` is itself a small dimensional model -- entity tables that describe
current configuration (an agent, an application, a schedule) beside event tables that
record something that happened (a run, a message, a tool call). The two need
different treatment, and the split below is the registry's central judgement call:

* **`MODE_SNAPSHOT`** -- the whole table, re-read and re-landed in full under today's
  partition, every day. Right for a table that is small, changes rarely, and whose
  interesting fact is "what does this look like *now*" rather than "when did this
  change": `agents`, `applications`, `schedules`, `mcp_servers`, and so on. A snapshot
  can never silently miss an update, because there is no filter to miss it with --
  the cost is a small amount of daily-repeated Parquet, which at these row counts
  (dozens to low thousands) is noise next to `usage_events`.
* **`MODE_WINDOW`** -- one partition per UTC day of a chosen `change_column`, and a
  trailing re-extraction window on every run (see `WINDOW_DAYS` below). Right for a
  table that is genuinely event-shaped: `usage_events`, `run_steps`, `messages`,
  `runs`. A row here is written once and, at most, updated a few times shortly after
  -- a run moves from `pending` to `completed`, an automation step gets its `outputs`
  -- and the trailing window exists to catch exactly that, not to chase a row that
  changes for months.

The choice is not always the only defensible one -- `api_keys` could plausibly be
windowed on `updated_at` to catch a revocation the same day it happens rather than
whenever the next snapshot runs -- but a config table with a few dozen rows costs
nothing to re-read whole, and "the whole table, every day" is a property a reader
never has to reason about, unlike "windowed, assuming nothing older than seven days
still changes."

## The trailing window, and why seven days

A `MODE_WINDOW` table is not read one day at a time. Each scheduled tick re-extracts
the **trailing `WINDOW_DAYS` days** -- today back through six days ago -- as
`WINDOW_DAYS` separate partition runs, each overwriting exactly one day's object
idempotently (see `homelab_datalake.assets.ai_platform.ai_platform_daily_schedule`).
That is what makes "a run goes pending -> completed" and "a conversation is
soft-deleted" land correctly without a stateful CDC feed: the row's `change_column`
places it on the day it was *created*, and if anything about it changes within the
next six days, the re-extraction on that later day's tick still finds it (the
`WHERE` clause below is against the row's own `change_column`, not against when the
scheduler runs) and overwrites the same partition with the current values.

Seven days is chosen, not derived: it comfortably covers the platform's own
observed lifecycles (a run typically completes in seconds to minutes; an automation
step run inside the same run; an approval is usually decided within a day; the
longest ordinary lag is a scheduled report or automation sitting `awaiting_input`
across a weekend). It does not claim to be permanent -- a `runs` row still
`awaiting_input_deadline` eight days out would land in the warehouse with whatever
values it had on day one and never be corrected by this mechanism. That is a real
and named limitation rather than a silent one; a backfill re-run is the fix, exactly
as for the camera late-arrival case this repo already carries.

## Never copied

Some columns are excluded outright, never selected and never granted. Two
enforcements make that a fact about the system rather than a promise in a comment:

1. **The registry excludes them.** `TableSpec.columns` is the SELECT list, named
   explicitly like every other source in this repo -- see `litellm_spend.py` for why
   `select *` is never used here. A column absent from it is never read.
2. **The generated grant is column-level.** `render_grant_sql()` builds
   `GRANT SELECT (col, col, ...) ON table TO datalake_ro` from `TableSpec.columns`
   alone, so even a bug that somehow asked for an excluded column would be refused by
   Postgres itself. `test_ai_platform_registry.py` asserts the checked-in
   `infra/ai-platform-datalake-role.sql` is exactly what this function renders today,
   so the file cannot drift from the code that is supposed to have generated it.

The excluded columns, and why each one is out:

* `api_keys.secret_hash`, `.secret_ciphertext`, `.litellm_virtual_key_id` -- a
  credential and a pointer to one. `label`, `prefix` and `suffix` are what a person
  recognises a key by and travel in the clear, same as everywhere else in this repo
  a credential sits beside its human-readable alias.
* `providers.credential_ciphertext` -- the encrypted API key for an upstream model
  provider. `credential_fingerprint` (already a hash) is what is kept.
* `push_subscriptions.endpoint`, `.p256dh`, `.auth` -- a Web Push endpoint and its
  keys are, functionally, a bearer credential for sending notifications to a
  specific device.
* `platform_settings.ntfy_topic` -- a push-notification topic name is a capability:
  anyone who has it can publish to that device.
* `user_tool_configs.values` -- per-user tool configuration can hold anything a tool
  needs to remember, up to and including a credential a person pasted into a form.
  Nothing downstream can tell a stored preference from a stored secret, so the whole
  column is treated as one.
* `mcp_servers.command` -- a shell command line for a locally-spawned MCP server,
  which routinely embeds an API key or token as an argument or environment setting.
* `mcp_servers.metadata` -- a free-form JSON bag next to `command`; the same
  reasoning applies, and nothing constrains what a server integration puts in it.
* `mcp_servers.url` -- kept, not excluded, but landed with its query string removed
  by `homelab_datalake.privacy.strip_query`, the same boundary the NPM and camera
  sources already use: a remote MCP server's URL routinely carries an API key or
  session token as a query parameter.

Two more columns are excluded for a different reason -- not secrecy, but shape.
`document_chunks.embedding` and `media_items.embedding` are pgvector columns: high-
dimensional float arrays with no SQL-analytical meaning (nobody asks "what is the
average embedding") and no compact Parquet representation. Landing them would cost
real storage for a capability -- vector search -- this warehouse does not offer.
Declared here, the same as the secrets, so a reader asking "where did the embedding
go" gets a documented answer rather than a silent gap.

`_prisma_migrations` and `luna_reprice_backup_20260808` are skipped entirely: the
first is Prisma's own migration ledger, not application data; the second is a
one-off backup table, named for the day it was taken, and not a source anybody
should be building a recurring pipeline against.
"""

from __future__ import annotations

from dataclasses import dataclass, field

MODE_SNAPSHOT = "snapshot"
MODE_WINDOW = "window"

# See the module docstring: not derived, chosen to comfortably cover this platform's
# observed row lifecycles with room to spare.
WINDOW_DAYS = 7

# Tables that exist in `ai_platform` and are deliberately never ingested. Kept as a
# named constant, not just absent from the registry, because "should this be added"
# is a question worth being able to answer by reading one place.
SKIPPED_TABLES: frozenset[str] = frozenset({
    "_prisma_migrations",       # Prisma's own migration ledger, not application data.
    "luna_reprice_backup_20260808",  # a one-off backup copy, not a recurring source.
})


@dataclass(frozen=True)
class ExcludedColumn:
    """One column this table deliberately never selects, and why."""

    column: str
    reason: str
    # True for the columns named in the "never copied" hard rule above. Read by the
    # secret-scan test to make sure every one of them is actually represented here,
    # rather than a column being quietly dropped from `columns` with no record of why.
    secret: bool = False


@dataclass(frozen=True)
class TableSpec:
    """One `ai_platform` table, and everything the pipeline needs to know about it.

    `name` is the table's name in `ai_platform`; the lake and the warehouse know it
    as `platform_<name>` (see `homelab_datalake.layout`), so that a reader can tell at
    a glance which tables came from this source without a second lookup.
    """

    name: str
    primary_key: str  # comma-separated for a composite key, e.g. "user_id,team_id"
    mode: str  # MODE_SNAPSHOT or MODE_WINDOW
    description: str
    # The exact SELECT list, in the order they are read -- never `select *`, for the
    # same reason `litellm_spend.py` gives: a new column added upstream should arrive
    # as a deliberate change here, not appear in the warehouse on its own.
    columns: tuple[str, ...]
    # Required when mode is MODE_WINDOW; ignored (must be None) for MODE_SNAPSHOT,
    # which re-reads the whole table on every run and has no filter to apply.
    change_column: str | None = None
    excluded_columns: tuple[ExcludedColumn, ...] = ()
    # The name of a bytea column inside `columns` whose bytes are written to the lake
    # as an object and replaced, in the landed row, by `object_key`. Still selected
    # from Postgres -- the bytes have to be read to be written somewhere -- but never
    # landed in Parquet.
    binary_column: str | None = None
    # Columns inside `columns` whose value is run through
    # `homelab_datalake.privacy.strip_query` before landing. Only `mcp_servers.url`
    # today.
    redact_query_columns: tuple[str, ...] = field(default_factory=tuple)
    # Set only for `usage_events`: the metadata column undergoes the frame-decoding
    # treatment in `homelab_datalake.parsing.ai_platform` rather than landing as-is.
    decode_metadata_images: bool = False

    def __post_init__(self) -> None:
        if self.mode == MODE_WINDOW and not self.change_column:
            raise ValueError(f"{self.name}: MODE_WINDOW requires a change_column")
        if self.mode == MODE_SNAPSHOT and self.change_column:
            raise ValueError(f"{self.name}: MODE_SNAPSHOT takes no change_column")
        if self.binary_column and self.binary_column not in self.columns:
            raise ValueError(f"{self.name}: binary_column must be one of columns")
        for name in self.redact_query_columns:
            if name not in self.columns:
                raise ValueError(f"{self.name}: redact_query_columns must be in columns")

    @property
    def table_name(self) -> str:
        """The name this table has in the lake and the warehouse."""
        return f"platform_{self.name}"


def _w(name, pk, change_column, columns, description, **kw) -> TableSpec:
    return TableSpec(
        name=name, primary_key=pk, mode=MODE_WINDOW, change_column=change_column,
        columns=columns, description=description, **kw,
    )


def _s(name, pk, columns, description, **kw) -> TableSpec:
    return TableSpec(
        name=name, primary_key=pk, mode=MODE_SNAPSHOT, columns=columns,
        description=description, **kw,
    )


# --------------------------------------------------------------------------------
# MODE_SNAPSHOT: small, config- and entity-shaped tables. Re-read whole, daily.
# --------------------------------------------------------------------------------

_SNAPSHOT_TABLES: tuple[TableSpec, ...] = (
    _s("agents", "id", (
        "id", "owner_user_id", "team_id", "name", "description", "status",
        "current_version", "created_at", "updated_at", "template_id",
    ), "Every agent configured on the platform, as it stands today."),

    _s("api_keys", "id", (
        "id", "application_id", "created_by_user_id", "label", "prefix", "suffix",
        "status", "last_used_at", "revoked_at", "created_at", "updated_at",
    ), "API keys issued to applications. The secret itself is never selected -- see "
       "the module docstring.", excluded_columns=(
        ExcludedColumn("secret_hash", "the key's hash; a working credential.", secret=True),
        ExcludedColumn("secret_ciphertext", "the key, encrypted; still a credential.", secret=True),
        ExcludedColumn("litellm_virtual_key_id", "a pointer to the matching LiteLLM key.",
                        secret=True),
    )),

    _s("application_owners", "application_id,user_id", (
        "application_id", "user_id", "created_at",
    ), "Which users own which applications. A small roster, not an event log."),

    _s("applications", "id", (
        "id", "created_by_user_id", "team_id", "name", "environment", "status",
        "metadata", "created_at", "updated_at", "model_aliases", "tool_profiles",
        "monthly_budget_eur", "last_used_at", "parent_application_id",
    ), "Every application registered against the gateway, with its budget and model "
       "routing configuration."),

    _s("automation_steps", "id", (
        "id", "automation_id", "position", "type", "label", "config", "created_at",
        "updated_at",
    ), "The step definitions inside each automation -- what runs, not runs of it."),

    _s("automations", "id", (
        "id", "owner_user_id", "name", "description", "enabled", "cadence_cron",
        "timezone", "created_by_approval_id", "created_at", "updated_at",
        "consecutive_failures", "paused_reason", "paused_at", "last_error",
    ), "Every configured automation and its current health."),

    _s("budgets", "id", (
        "id", "created_by_user_id", "name", "scope_type", "scope_id", "period",
        "amount_eur", "alert_threshold_percent", "hard_limit", "status", "metadata",
        "created_at", "updated_at",
    ), "Spend budgets and their scope, as configured today."),

    _s("document_tags", "id", (
        "id", "owner_user_id", "name", "slug", "curated", "created_by_run_id",
        "created_at", "updated_at",
    ), "The tag vocabulary applied to generated files and documents."),

    _s("email_recipients", "id", (
        "id", "owner_user_id", "label", "address", "notes", "archived_at",
        "created_at", "updated_at",
    ), "The address book automations and reports send email to."),

    _s("generated_file_tags", "id", (
        "id", "generated_file_id", "document_tag_id", "assigned_by", "run_id",
        "created_at",
    ), "Which tags are attached to which generated files."),

    _s("index_build_states", "index_key", (
        "index_key", "status", "started_at", "finished_at", "last_successful_at",
        "indexed", "skipped", "failed", "error", "cost_eur", "embed_tokens",
        "updated_at",
    ), "The health of each embedding index build (media, documents) -- a handful of "
       "named rows, not an event stream."),

    _s("mcp_servers", "id", (
        "id", "created_by_user_id", "name", "transport", "url", "status",
        "tool_count", "allowed_tools", "auth_fingerprint", "auth_updated_at",
        "created_at", "updated_at",
    ), "Every MCP server registered with the platform, and what it is trusted to do.",
       redact_query_columns=("url",), excluded_columns=(
        ExcludedColumn("command", "a local server's launch command; routinely carries "
                        "a credential as an argument or environment value.", secret=True),
        ExcludedColumn("metadata", "a free-form bag beside `command`; same reasoning.",
                        secret=True),
    )),

    _s("model_routes", "id", (
        "id", "provider_id", "created_by_user_id", "alias", "display_name",
        "provider_model_id", "mode", "privacy", "status", "context_window_tokens",
        "input_cost_eur_per_1m", "output_cost_eur_per_1m", "allowed_roles",
        "fallback_policy", "metadata", "created_at", "updated_at",
    ), "Every model route/alias and its pricing, as configured on the gateway."),

    _s("platform_settings", "id", (
        "id", "default_chat_model", "default_approval_policy", "notification_email",
        "data_retention_days", "feature_flags", "created_at", "updated_at",
        "notification_events", "media_index_enabled", "media_embedding_model",
        "media_index_cadence_hours", "media_default_max_retrieve",
        "media_include_watched_default", "media_query_prefix", "media_document_prefix",
        "documents_index_enabled", "documents_embedding_model", "documents_chunk_size",
        "documents_chunk_overlap", "documents_query_prefix", "documents_document_prefix",
    ), "The platform's own settings. One row, snapshotted rather than windowed because "
       "a single row has no meaningful 'day it changed' worth partitioning on.",
       excluded_columns=(
        ExcludedColumn("ntfy_topic", "a push-notification topic name is a capability: "
                        "anyone holding it can publish to the owner's device.", secret=True),
    )),

    _s("providers", "id", (
        "id", "created_by_user_id", "name", "kind", "base_url", "status",
        "credential_fingerprint", "credential_updated_at", "capabilities", "metadata",
        "created_at", "updated_at",
    ), "Every upstream model provider configured, with a fingerprint of its "
       "credential rather than the credential.", excluded_columns=(
        ExcludedColumn("credential_ciphertext", "the encrypted provider API key.",
                        secret=True),
    )),

    _s("push_subscriptions", "id", (
        "id", "user_id", "user_agent", "created_at",
    ), "Which devices are subscribed to push notifications, and since when. The "
       "subscription's own endpoint and keys are a bearer credential for sending to "
       "that device and are never selected.", excluded_columns=(
        ExcludedColumn("endpoint", "the Web Push endpoint URL; a send credential.",
                        secret=True),
        ExcludedColumn("p256dh", "the subscription's public key; part of the "
                        "credential.", secret=True),
        ExcludedColumn("auth", "the subscription's auth secret; part of the "
                        "credential.", secret=True),
    )),

    _s("report_alert_rules", "id", (
        "id", "owner_user_id", "template_id", "metric_key", "comparator", "threshold",
        "severity", "enabled", "created_at", "updated_at", "report_definition_id",
        "consecutive_runs", "retired_at",
    ), "Alert thresholds configured against report metrics."),

    _s("report_definitions", "id", (
        "id", "owner_user_id", "template_id", "agent_id", "name", "period",
        "timezone", "comparison_enabled", "enabled_sections", "advanced_instruction",
        "notify_on_ready", "notify_on_failure", "delivery_channels", "created_at",
        "updated_at", "language",
    ), "Every configured recurring report, and how it is delivered."),

    _s("sandbox_policy", "id", (
        "id", "enabled", "min_input_rows", "max_execution_ms", "max_cpu_seconds",
        "max_memory_mb", "max_output_bytes", "max_code_bytes", "max_concurrent_runs",
        "max_runs_per_hour", "allowed_modules", "created_at", "updated_at",
    ), "The code sandbox's own limits. One row, snapshotted for the same reason as "
       "platform_settings."),

    _s("schedules", "id", (
        "id", "owner_user_id", "agent_id", "prompt", "cadence", "hour", "minute",
        "weekday", "enabled", "next_run_at", "last_run_at", "last_run_id",
        "last_status", "created_at", "updated_at", "report_definition_id", "timezone",
        "consecutive_failures", "paused_reason", "paused_at", "last_error",
    ), "Every scheduled prompt, and its health."),

    _s("scripts", "id", (
        "id", "owner_user_id", "name", "description", "current_version",
        "archived_at", "created_at", "updated_at", "kind",
    ), "Saved sandbox scripts. The code itself lives in script_versions, windowed."),

    _s("teams", "id", (
        "id", "keycloak_group_path", "display_name", "slug", "litellm_team_alias",
        "litellm_team_id", "created_at", "updated_at",
    ), "Every team, and its mapping to a Keycloak group and a LiteLLM team."),

    _s("team_members", "user_id,team_id", (
        "user_id", "team_id", "source", "joined_at",
    ), "Team membership -- a small roster, not an event log."),

    _s("tool_definitions", "id", (
        "id", "base_name", "description", "category", "risk", "risk_reason",
        "user_config_fields", "created_at", "updated_at",
    ), "The catalogue of tools the platform knows about, and their risk rating."),

    _s("user_preferences", "user_id", (
        "user_id", "theme", "locale", "timezone", "preferences", "updated_at",
    ), "Per-user display and locale preferences. One row per user."),

    _s("user_roles", "user_id,role", (
        "user_id", "role", "source", "assigned_at",
    ), "Which platform roles each user holds -- a small roster."),

    _s("user_tool_configs", "id", (
        "id", "user_id", "tool_base_name", "variant", "created_at", "updated_at",
        "confirm_before_use",
    ), "Per-user tool configuration. `values` is never selected -- it can hold "
       "anything a tool integration needs to remember, credentials included.",
       excluded_columns=(
        ExcludedColumn("values", "per-user tool configuration values; nothing "
                        "downstream can tell a stored preference from a stored "
                        "secret, so the column is treated as one.", secret=True),
    )),

    _s("users", "id", (
        "id", "keycloak_subject", "username", "display_name", "email", "status",
        "litellm_end_user_id", "first_seen_at", "last_seen_at", "last_login_at",
        "created_at", "updated_at",
    ), "Every platform user. A handful of rows -- this is a household's own "
       "platform, not a multi-tenant service -- snapshotted rather than windowed."),
)


# --------------------------------------------------------------------------------
# MODE_WINDOW: event- and log-shaped tables. Trailing-window re-extraction.
# --------------------------------------------------------------------------------

_WINDOW_TABLES: tuple[TableSpec, ...] = (
    _w("agent_versions", "id", "created_at", (
        "id", "agent_id", "version", "created_by_user_id", "definition", "created_at",
    ), "Every saved version of every agent's definition. Immutable once written -- a "
       "new version is a new row -- so the window exists only for safety margin."),

    _w("approvals", "id", "updated_at", (
        "id", "run_id", "requested_by_user_id", "decided_by_user_id", "tool_name",
        "risk", "status", "purpose", "target", "arguments", "expires_at",
        "decided_at", "created_at", "updated_at",
    ), "Every approval request and its outcome -- pending, then approved or denied."),

    _w("audit_events", "id", "occurred_at", (
        "id", "actor_user_id", "actor_subject", "action", "resource_type",
        "resource_id", "result", "request_id", "ip_address", "user_agent",
        "metadata", "occurred_at",
    ), "Every audited action on the platform: who did what, to what, and the "
       "outcome. Append-only, so the trailing window is pure safety margin."),

    _w("automation_runs", "id", "created_at", (
        "id", "automation_id", "dagster_run_id", "status", "started_at",
        "finished_at", "created_at",
    ), "One row per run of an automation, from pending through its outcome."),

    _w("automation_step_runs", "id", "started_at", (
        "id", "automation_run_id", "step_id", "dagster_run_id", "status", "outputs",
        "error", "started_at", "finished_at", "step_position", "step_type",
        "step_label", "step_config",
    ), "One row per step of one automation run."),

    _w("conversations", "id", "updated_at", (
        "id", "owner_user_id", "team_id", "title", "agent_id", "created_at",
        "updated_at", "deleted_at", "reasoning_level", "origin",
    ), "Every conversation. `deleted_at` marks a soft delete rather than removing "
       "the row, so it lands here too."),

    _w("document_chunks", "id", "indexed_at", (
        "id", "source", "chunk_key", "section", "content", "content_hash",
        "indexed_at",
    ), "Indexed chunks of the platform's document corpus, with their text. "
       "`embedding` is excluded -- see the module docstring.", excluded_columns=(
        ExcludedColumn("embedding", "a pgvector embedding: no SQL-analytical meaning "
                        "and no compact Parquet form; this warehouse offers no "
                        "vector search to justify the storage."),
    )),

    _w("email_sends", "id", "created_at", (
        "id", "owner_user_id", "run_id", "approval_id", "template_script_id",
        "template_version", "subject", "recipients", "attachments", "status",
        "failure_reason", "created_at",
    ), "Every email the platform has sent or attempted to send, with its recipients "
       "and outcome."),

    _w("generated_files", "id", "created_at", (
        "id", "conversation_id", "run_id", "filename", "mime_type", "byte_size",
        "data", "created_at", "tool_name", "server_alias",
    ), "Files generated during a run -- workbooks, documents, charts. The bytes are "
       "landed as one lake object per file; the row carries `object_key` instead of "
       "`data`.", binary_column="data"),

    _w("incidents", "id", "last_seen_at", (
        "id", "fingerprint", "status", "name", "summary", "severity", "occurrences",
        "opened_at", "last_seen_at", "resolved_at",
    ), "Operational incidents opened against the platform, including those the "
       "Dagster failure sensor in this repo itself opens."),

    _w("media_items", "id", "indexed_at", (
        "id", "jellyfin_item_id", "title", "year", "genres", "overview",
        "community_rating", "content_hash", "indexed_at", "cast_line", "media_type",
    ), "The platform's own media index -- public film metadata, mirroring what the "
       "Jellyfin source lands independently. `embedding` is excluded -- see the "
       "module docstring.", excluded_columns=(
        ExcludedColumn("embedding", "a pgvector embedding; same reasoning as "
                        "document_chunks.embedding."),
    )),

    _w("message_attachments", "id", "created_at", (
        "id", "conversation_id", "message_id", "uploaded_by_user_id", "mime_type",
        "byte_size", "data", "created_at",
    ), "Files a person attached to a message. Landed the same way as "
       "generated_files: one object per attachment, `object_key` in place of `data`.",
       binary_column="data"),

    _w("messages", "id", "created_at", (
        "id", "conversation_id", "author_user_id", "run_id", "role", "content",
        "metadata", "created_at",
    ), "Every message in every conversation, full text, by the owner's 2026-09-14 "
       "decision -- see the module docstring."),

    _w("notifications", "id", "created_at", (
        "id", "user_id", "kind", "severity", "subject", "body", "channel_results",
        "read_at", "run_id", "schedule_id", "incident_id", "created_at", "report_id",
    ), "Every notification the platform has raised, and whether it was read."),

    _w("report_alerts", "id", "created_at", (
        "id", "rule_id", "report_id", "owner_user_id", "metric_key", "metric_label",
        "unit", "value", "threshold", "comparator", "severity", "created_at",
    ), "Every time a report_alert_rule actually fired."),

    _w("reports", "id", "created_at", (
        "id", "owner_user_id", "run_id", "agent_id", "template_id", "title",
        "summary", "period_start", "period_end", "payload_version", "payload",
        "created_at", "metrics", "report_definition_id", "dataset_version",
        "layout_version", "normalized_data", "insights", "archived_at",
    ), "Every generated report, with its full payload and computed insights."),

    _w("resource_mappings", "id", "updated_at", (
        "id", "provider", "resource_type", "local_resource_type",
        "local_resource_id", "external_id", "metadata", "created_at", "updated_at",
    ), "Cross-references between a local record and an id in an external system."),

    _w("run_steps", "id", "occurred_at", (
        "id", "run_id", "sequence", "type", "title", "description", "payload",
        "occurred_at",
    ), "Every step of every run: tool calls, reasoning steps, approvals asked for. "
       "The source for fct_agent_tool_call. High volume -- [count withheld] tool calls in the "
       "trailing week alone when this was measured -- and never passes through the "
       "LLM gateway, so litellm_spend does not see it."),

    _w("runs", "id", "started_at", (
        "id", "conversation_id", "initiated_by_user_id", "status",
        "requested_model", "actual_route", "input_tokens", "output_tokens",
        "cost_eur", "started_at", "completed_at", "failure_code", "metadata",
        "awaiting_input_deadline", "report_definition_id", "reasoning_tokens",
    ), "Every agent run -- one per turn of work, whether or not it called the "
       "gateway. The source for fct_agent_run."),

    _w("sandbox_executions", "id", "created_at", (
        "id", "run_id", "owner_user_id", "status", "dataset", "input_rows",
        "wall_time_ms", "output_bytes", "failure_code", "created_at", "completed_at",
    ), "Every code-sandbox execution, with its resource usage and outcome."),

    _w("schedule_runs", "id", "created_at", (
        "id", "schedule_id", "run_id", "status", "ran_at", "created_at",
    ), "One row per firing of a schedule."),

    _w("script_versions", "id", "created_at", (
        "id", "script_id", "version", "code", "parameters", "author_user_id",
        "created_at", "tags", "domain", "guidance", "return_contract",
        "dependencies", "verified_at",
    ), "Every saved version of every sandbox script, code included."),

    _w("tool_grants", "id", "granted_at", (
        "id", "conversation_id", "user_id", "category", "granted_at", "expires_at",
        "revoked_at", "source_excerpt",
    ), "Every tool-category grant made inside a conversation, and its lifetime."),

    _w("usage_events", "id", "created_at", (
        "id", "application_id", "api_key_id", "source", "client_name",
        "client_version", "actor_label", "actor_external_id", "requested_model",
        "actual_model", "prompt", "response", "results", "input_tokens",
        "output_tokens", "cost_eur", "latency_ms", "status", "failure_code",
        "metadata", "created_at", "conversation_id",
    ), "Every call through the api.ai gateway: application, key, person, requested "
       "and actual model, full prompt and response, tokens, cost, latency, status, "
       "and any camera frames attached. The largest table by far -- [count withheld] calls "
       "and [volume withheld] when this was measured, most of it [count withheld] base64-encoded camera "
       "frames in `metadata`, which are decoded into lake objects rather than "
       "landed as text -- see `homelab_datalake.parsing.ai_platform`.",
       decode_metadata_images=True),
)

REGISTRY: tuple[TableSpec, ...] = _SNAPSHOT_TABLES + _WINDOW_TABLES

_BY_NAME: dict[str, TableSpec] = {t.name: t for t in REGISTRY}

if len(_BY_NAME) != len(REGISTRY):
    # A duplicate table name in the tuples above would silently shadow one spec with
    # another everywhere this dict is used. Fail at import time, not at query time.
    _seen: set[str] = set()
    _dupes = sorted({t.name for t in REGISTRY if t.name in _seen or _seen.add(t.name)})
    raise RuntimeError(f"duplicate TableSpec name(s) in the ai_platform registry: {_dupes}")


def spec(name: str) -> TableSpec:
    try:
        return _BY_NAME[name]
    except KeyError:
        raise KeyError(
            f"no ai_platform table registered as {name!r}. Known: {sorted(_BY_NAME)}"
        ) from None


# --------------------------------------------------------------------------------
# Coverage: does the registry match the live database?
# --------------------------------------------------------------------------------


def registry_coverage(live_tables: list[str]) -> tuple[list[str], list[str]]:
    """(missing, stale) against a live `information_schema.tables` listing.

    `missing` is a table that exists in `ai_platform` today, is not one of the two
    deliberately skipped tables, and has no `TableSpec` -- a table added to the
    platform after this registry was written, which would otherwise never arrive in
    the warehouse and never be noticed, because nothing fails: the pipeline simply
    keeps landing exactly what it always has.

    `stale` is the opposite direction: a `TableSpec` naming a table that is no
    longer there, which usually means the table was dropped or renamed upstream and
    the registry was not updated to match.
    """
    live = set(live_tables) - SKIPPED_TABLES
    registered = set(_BY_NAME)
    missing = sorted(live - registered)
    stale = sorted(registered - live)
    return missing, stale


# --------------------------------------------------------------------------------
# The generated grant SQL. See infra/ai-platform-datalake-role.sql.
# --------------------------------------------------------------------------------

GRANT_SQL_HEADER = """\
-- GENERATED FILE. Do not hand-edit.
--
-- Rendered from homelab_datalake.parsing.ai_platform_registry.render_grant_sql().
-- test_ai_platform_registry.py asserts this file is exactly what that function
-- renders today, so a registry change that is not accompanied by regenerating this
-- file fails the test suite rather than silently drifting.
--
-- Run on the ai_platform Postgres as a superuser (or a role with GRANT privilege).
-- CONNECTION LIMIT 3: the pipeline opens one connection per run and closes it
-- immediately (see ai_platform.py), and the server this runs on is shared --
-- max_connections is 100, with keycloak, banking-tracker, litellm and dagster all
-- drawing on it too.

CREATE ROLE datalake_ro LOGIN CONNECTION LIMIT 3 PASSWORD '<set at provisioning time>';
GRANT CONNECT ON DATABASE ai_platform TO datalake_ro;
GRANT USAGE ON SCHEMA public TO datalake_ro;
"""


def render_grant_sql(registry: tuple[TableSpec, ...] = REGISTRY) -> str:
    """The role-provisioning SQL, built from the registry alone.

    Column-level GRANT, not table-level: `GRANT SELECT ON table TO role` would grant
    every column including the ones `TableSpec.columns` deliberately leaves out, so a
    bug in the extraction code -- not the registry -- could still read a secret. This
    way the database itself refuses the column even if the Python code asked for it.
    """
    lines = [GRANT_SQL_HEADER]
    for table in sorted(registry, key=lambda t: t.name):
        columns = ", ".join(table.columns)
        lines.append(f'GRANT SELECT ({columns}) ON "{table.name}" TO datalake_ro;')
    lines.append("")  # trailing newline
    return "\n".join(lines)
