"""Where every object lives in the lake, and why it lives there.

The bucket has three layers, and the layer is the first path segment. This is the
medallion pattern under plainer names, and it exists so that the question "is this
number raw or is it derived?" is answered by the path rather than by reading code:

    raw/       As landed from the source. One object per source record set, per
               partition. Never rewritten except by re-running its own partition.
    curated/   The same records, cleaned, typed, deduplicated and enriched. This is
               the layer to point a notebook, a BI tool or another team at.
    marts/     Business objects: facts, dimensions and daily aggregates. Answers
               rather than records.

Below the layer, the path is `<domain>/<source>/<dataset>`. Two sources that happen
to be daily Parquet do not belong in one directory just because they are both daily
Parquet -- `jellyfin_titles` and `camera_clips` describe unrelated things, are owned
by different applications, and are read by different people. Grouping by domain and
then by the system that produced it is what makes the bucket navigable by somebody
who has never seen this repository.

One caveat is worth stating because it looks like a violation and is not. `raw/`
carries the geo and network columns even though those are derived, and they cannot
be moved to `curated/`. Geo is resolved from the client address, and the client
address is destroyed in memory at ingest and never written anywhere (see
homelab_datalake.privacy). Deriving it later would mean keeping addresses on disk to
derive it from, which is the leak the privacy boundary exists to prevent. So the
rule is: `raw/` is what the source told us plus what could only be learned at the
moment of ingest. Everything else derived is in `curated/`.

Nothing in this module reads or writes. It is the single place the path strings are
built, so a rename is one edit and the migration script can see both the old name
and the new one.
"""

from __future__ import annotations

from dataclasses import dataclass

from homelab_datalake.parsing import ai_platform_registry as _ai_platform
from homelab_datalake.parsing import finance_tables as _finance_tables
from homelab_datalake.parsing import media_pipeline as _media_pipeline

LAYER_RAW = "raw"
LAYER_CURATED = "curated"
LAYER_MARTS = "marts"

LAYERS = (LAYER_RAW, LAYER_CURATED, LAYER_MARTS)

# Domains. A domain answers "who asks questions about this", not "which container
# produced it". Camera and reverse-proxy logs are both operational telemetry, but
# nobody looks at them together, so they are not one domain.
DOMAIN_INFRASTRUCTURE = "infrastructure"  # the edge and the machines behind it: traffic, metrics, host logs, sign-ins
DOMAIN_MEDIA = "media"                    # the film library and what describes it
DOMAIN_MONITORING = "monitoring"          # cameras, zones, activity
DOMAIN_AI = "ai"                          # model calls and what they cost


@dataclass(frozen=True)
class Dataset:
    """One directory of Parquet in the lake.

    `table` is the name the same data has in DuckDB, and it is what joins this
    registry to the warehouse and to dbt. `partition_column`, when set, is the date
    column the objects are split on -- it is a column of the data, not an extra
    field, so a reader that globs the whole prefix still sees it.
    """

    table: str
    layer: str
    domain: str
    source: str
    name: str
    description: str
    partition_column: str | None = None
    # The path this dataset used before the layers existed. Kept so the migration
    # can be re-run and can be checked, and so a deployment mid-migration can still
    # find its data. Remove these once every environment is on the new layout.
    legacy_prefix: str | None = None
    # Why this dataset is NOT loaded into the DuckDB warehouse, or empty when it is.
    # A dataset of tens of millions of rows a day (Prometheus samples, the journal)
    # would grow a file that is rebuilt in full at least once a day; it stays
    # queryable through the SQL API, which reads every lake dataset directly. A
    # non-empty reason, not a bare flag, so the exclusion explains itself.
    # Non-empty names the reason this dataset should not be rebuilt into the nightly
    # DuckDB warehouse the way every other raw table is -- unbounded growth that
    # would make `warehouse_tables`' full-history `read_parquet` rescan slower and
    # larger every night, forever, with no ceiling. No dataset carries this today:
    # `platform_usage_events` looked like a candidate and was measured instead of
    # assumed -- see the note beside `_AI_PLATFORM_RAW` below -- and the real number
    # did not support it. Left in place for the next table that actually earns it.
    lake_only: str = ""
    # True when every object under this prefix is written from one fixed pyarrow
    # schema -- the writer builds the Arrow table with an explicit `schema=`
    # constant on every call, not from whatever columns happened to be present that
    # day. `homelab_datalake.api.query.LakeSession` uses this to skip
    # `union_by_name` when it binds the dataset's view: measured on the production
    # incident of 2026-09-16, `union_by_name=true` reads every matching file's
    # Parquet footer *at bind time* to compute the union schema -- 18.0s across
    # 1,805 files for raw.jellyfin_playback alone -- while a plain `read_parquet`
    # over a glob whose files are already known to share one schema binds from a
    # single file and defers the rest to query time. Verified directly (not
    # inferred from documentation): a `CREATE VIEW ... read_parquet(glob,
    # union_by_name=true)` over a glob containing one corrupt file fails at CREATE
    # VIEW time, because it opens every file's footer before the statement
    # returns; the same statement without `union_by_name` succeeds, because it
    # does not.
    #
    # This is true for `jellyfin_playback` and `jellyfin_activity` today -- both
    # are written exclusively through `project(..., _PLAYBACK_SCHEMA)` /
    # `project(..., _ACTIVITY_SCHEMA)` in `homelab_datalake.assets.jellyfin`, and
    # `project()` conforms every row to that schema before the Arrow table is ever
    # built, so a partition with an extra or missing column cannot reach the lake.
    # It is deliberately NOT set for anything dbt builds (every curated and marts
    # dataset): a dbt model's output columns change when the model changes, a
    # publish rewrites every partition of a table in one run, and a query can land
    # mid-run, while dbt has rewritten some of that table's partition files under
    # the new column set and not yet reached the rest -- `union_by_name` is what
    # keeps that transient mix queryable instead of raising a schema-mismatch
    # error. It is also not set for `ai_platform`'s registry tables, which gain
    # columns as the platform grows. Flip it only for a dataset whose writer is
    # Python code with one hardcoded pyarrow schema, never for one built by SQL.
    single_schema: bool = False

    @property
    def prefix(self) -> str:
        # `source` is empty for a mart, which belongs to no single source. Joining
        # only the segments that exist keeps that from becoming a `//` in the path,
        # which S3 treats as a real empty directory name rather than ignoring.
        parts = (self.layer, self.domain, self.source, self.name)
        return "/".join(part for part in parts if part)

    @property
    def qualified_name(self) -> str:
        """`<layer>.<table>` -- how the SQL console and the warehouse name it."""
        return f"{self.layer}.{self.table}"


def _raw(table, domain, source, name, description, partition_column="dt", lake_only="",
         single_schema=False):
    return Dataset(
        table=table, layer=LAYER_RAW, domain=domain, source=source, name=name,
        description=description, partition_column=partition_column,
        legacy_prefix=f"raw/{table}", lake_only=lake_only, single_schema=single_schema,
    )


def _curated(table, domain, source, name, description, partition_column):
    return Dataset(
        table=table, layer=LAYER_CURATED, domain=domain, source=source, name=name,
        description=description, partition_column=partition_column,
    )


def _mart(table, domain, name, description, partition_column):
    return Dataset(
        table=table, layer=LAYER_MARTS, domain=domain, source="", name=name,
        description=description, partition_column=partition_column,
    )


# --------------------------------------------------------------------------------
# raw: written by the ingest assets in homelab_datalake.assets
# --------------------------------------------------------------------------------

RAW_DATASETS: tuple[Dataset, ...] = (
    _raw("npm_access", DOMAIN_INFRASTRUCTURE, "npm", "access",
         "One row per HTTP request seen by an Nginx Proxy Manager instance."),
    _raw("npm_access_rejects", DOMAIN_INFRASTRUCTURE, "npm", "access_rejects",
         "Lines no log format matched, kept with their text, as a quality signal."),
    _raw("litellm_spend", DOMAIN_AI, "litellm", "spend",
         "One row per LLM call. Prompt and completion payloads are never selected."),
    _raw("jellyfin_titles", DOMAIN_MEDIA, "jellyfin", "titles",
         "One row per film per snapshot date. No paths, no playback history."),
    _raw("jellyfin_title_genres", DOMAIN_MEDIA, "jellyfin", "title_genres",
         "Title-to-genre pairs, kept separate because a title has many genres."),
    # Same reason as genres, twice over: keeping the first audio stream described a
    # bilingual film as monolingual, and keeping no subtitle stream at all described a
    # library with thirty-four tracks on one title as having none.
    _raw("jellyfin_title_streams", DOMAIN_MEDIA, "jellyfin", "title_streams",
         "One row per media stream: every audio, video and subtitle track."),
    _raw("jellyfin_title_people", DOMAIN_MEDIA, "jellyfin", "title_people",
         "Cast and crew credits. Public film metadata, not library users."),
    # The owner reversed the no-playback-history decision on 2026-09-14: the daily
    # recap email already reads this from the Playback Reporting plugin live, and the
    # platform's own Datalake Analyst had just refused a weekend watch-time question
    # for data the warehouse simply did not have. See parsing/jellyfin.py for what
    # changed and why.
    # single_schema=True for both: every partition is built through project(...,
    # _PLAYBACK_SCHEMA) / project(..., _ACTIVITY_SCHEMA) in assets/jellyfin.py, so
    # the writer cannot land a file with a different column set. See
    # Dataset.single_schema for what that buys LakeSession.
    _raw("jellyfin_playback", DOMAIN_MEDIA, "jellyfin", "playback",
         "Every Playback Reporting plugin row: who watched what, on what, for how "
         "long the player was open.", partition_column="partition_hour",
         single_schema=True),
    _raw("jellyfin_activity", DOMAIN_MEDIA, "jellyfin", "activity",
         "The server activity log: sign-ins, playback starts and stops, library and "
         "account changes.", single_schema=True),
    _raw("jellyfin_users", DOMAIN_MEDIA, "jellyfin", "users",
         "Daily snapshot of the account list: name, admin flag, disabled flag, last "
         "seen. Nothing from configuration or password policy beyond those."),
    # Added 2026-09-15: the movie-only snapshot above was never the whole library.
    # Series, seasons, episodes, box sets and other videos -- roughly 16,600 items --
    # land here instead of in jellyfin_titles; see assets/jellyfin.py for why they are
    # kept separate from the movie snapshot dim_title_scd2 depends on.
    _raw("jellyfin_library_items", DOMAIN_MEDIA, "jellyfin", "library_items",
         "Daily snapshot of everything in the library that is not a movie: series, "
         "seasons, episodes, box sets and other videos."),
    # Added when the AI Search plugin reached AiSearch/1.2.0.0 and started keeping a search
    # log: every /Items, /Search/Hints, /Persons and /Artists request Jellyfin itself
    # served, plus the plugin's own AI popup searches. Hourly, like jellyfin_playback,
    # and single_schema=True for the same reason -- both objects are written
    # exclusively through project(..., _SEARCH_SCHEMA) / project(...,
    # _SEARCH_RESULT_SCHEMA) in assets/jellyfin.py. See jellyfin_searches_raw's own
    # docstring for why there is no jellyfin_searches_history_raw counterpart: the
    # plugin only ever keeps retentionDays (7 by default), and nothing existed on the
    # server before this version shipped.
    _raw("jellyfin_searches", DOMAIN_MEDIA, "jellyfin", "searches",
         "Every search request the AI Search plugin logged: Jellyfin's own search "
         "endpoints and the plugin's own AI popup searches.",
         partition_column="partition_hour", single_schema=True),
    _raw("jellyfin_search_results", DOMAIN_MEDIA, "jellyfin", "search_results",
         "One row per result a logged search returned, capped at 250 per search by "
         "the plugin.", partition_column="partition_hour", single_schema=True),
    _raw("camera_events", DOMAIN_MONITORING, "camera", "events",
         "Attention events. Not final when first written: analysis lands later."),
    _raw("camera_clips", DOMAIN_MONITORING, "camera", "clips",
         "Clip metadata. No paths, playlists or thumbnails."),
    _raw("camera_logs", DOMAIN_MONITORING, "camera", "logs",
         "Activity log entries. Free-text titles are never exported."),
    # A daily snapshot rather than an event stream: a zone is configuration, and the
    # question asked of it is "how was it set up on the day this event fired", which
    # only a per-day copy can answer. Geometry is never exported -- see the camera's
    # export-routes.js for why a polygon of the property is not analytics.
    _raw("camera_zones", DOMAIN_MONITORING, "camera", "zones",
         "Attention-zone configuration per day: rules, thresholds, state. No geometry."),
    # The other half of every gate reading. Without these spans a stretch where the
    # camera could not see the zone is indistinguishable from a gate that stayed shut.
    _raw("camera_observability", DOMAIN_MONITORING, "camera", "observability",
         "Spans where a zone could not be read, with the reason it could not."),
    _raw("camera_annotations", DOMAIN_MONITORING, "camera", "annotations",
         "Notes and marks left against a clip or an event, with their metadata."),
    # One object, not a partition per day: TMDB is a lookup keyed by tmdb_id that is
    # rewritten in full, so a date in the path would only ever say when it was last
    # refreshed, which the object's own mtime already says.
    _raw("tmdb_titles", DOMAIN_MEDIA, "tmdb", "titles",
         "Budget, revenue, language and vote counts by TMDB id.",
         partition_column=None),
)

# The AI platform's own database: every api.ai gateway call, every agent run and
# tool call, every conversation, every automation run -- see
# homelab_datalake.parsing.ai_platform_registry for the full registry and the
# owner's 2026-09-14 decision to land all of it. One dataset per table, generated
# from the registry rather than written out by hand a second time here, so a
# table's description is never asked to agree with itself across two files.
# `dt` for every one of them, snapshot tables included: a snapshot still lands one
# object per day it was taken, the same as jellyfin_titles and camera_zones do.
_AI_PLATFORM_RAW: tuple[Dataset, ...] = tuple(
    _raw(spec.table_name, DOMAIN_AI, "ai_platform", spec.name, spec.description)
    for spec in _ai_platform.REGISTRY
)

# platform_usage_events carries full prompt and response text on every row. It
# looked like the one table in this registry that could grow past a nightly
# rebuild's reach -- 12 GB was measured against the whole table on 2026-09-14 -- but
# that figure was almost entirely the base64 camera frames, and those never reach
# this table at all: they are decoded to lake objects at ingestion, before the row
# is built (see ai_platform.py). Measured directly instead of assumed: across all
# 40,979 rows, prompt + response together are 28 MB, averaging 764 bytes a row, and
# the four most recent days ran 344-696 KB each. That is not a rebuild `raw.
# platform_usage_events` will ever notice, so it is loaded in full by
# `warehouse_tables` like every other table in the registry -- no `lake_only`.
RAW_DATASETS = RAW_DATASETS + _AI_PLATFORM_RAW

# --------------------------------------------------------------------------------
# curated: written by homelab_datalake.assets.publish, from the dbt models
# --------------------------------------------------------------------------------
#
# One dataset per source, not one per dbt model. Where an intermediate model exists
# it supersedes its staging model, so the lake does not carry the same rows twice
# under two names -- a reader asking "which of these two is the good one" is a
# failure of the layout, not a question worth answering in documentation.

CURATED_DATASETS: tuple[Dataset, ...] = (
    _curated("int_npm_requests", DOMAIN_INFRASTRUCTURE, "npm", "requests",
             ("Requests, typed and enriched: country, city, network operator, "
              "bot-versus-browser, and the path grouped to two segments."),
             "partition_date"),
    _curated("int_camera_events", DOMAIN_MONITORING, "camera", "events",
             ("Events with late-arriving analysis resolved, duplicates marked, and "
              "duration computed."),
             "partition_date"),
    _curated("stg_camera_clips", DOMAIN_MONITORING, "camera", "clips",
             "Clips, typed, with save latency and bitrate derived.", "partition_date"),
    _curated("stg_camera_logs", DOMAIN_MONITORING, "camera", "logs",
             "Activity log entries, typed.", "partition_date"),
    _curated("stg_camera_zones", DOMAIN_MONITORING, "camera", "zones",
             "Zone and rule configuration, typed, one row per rule.", "snapshot_date"),
    _curated("stg_camera_observability", DOMAIN_MONITORING, "camera", "observability",
             "Unreadable-zone spans, typed, with durations.", "partition_date"),
    _curated("stg_camera_annotations", DOMAIN_MONITORING, "camera", "annotations",
             "Annotations, typed, with their target resolved.", "partition_date"),
    _curated("stg_jellyfin_title_streams", DOMAIN_MEDIA, "jellyfin", "title_streams",
             "Media streams, typed.", "snapshot_date"),
    _curated("stg_jellyfin_title_people", DOMAIN_MEDIA, "jellyfin", "title_people",
             "Cast and crew credits, typed.", "snapshot_date"),
    _curated("stg_litellm_spend", DOMAIN_AI, "litellm", "spend",
             "Calls with the workload, provider and self-hosted flags resolved.",
             "partition_date"),
    _curated("stg_jellyfin_titles", DOMAIN_MEDIA, "jellyfin", "titles",
             "Titles, typed, with decade and resolution class derived.",
             "snapshot_date"),
    _curated("stg_jellyfin_title_genres", DOMAIN_MEDIA, "jellyfin", "title_genres",
             "Title-to-genre pairs, trimmed and deduplicated.",
             "snapshot_date"),
    # int_, not stg_: playback sessions are deduped onto (started_at, user_id,
    # item_id) here, the same reason int_camera_events supersedes stg_camera_events.
    _curated("int_jellyfin_playback", DOMAIN_MEDIA, "jellyfin", "playback",
             "Playback sessions, typed and deduped, with an episode's series "
             "resolved by id against the library-items snapshot where it exists, "
             "the title-parse kept only as a fallback.", "partition_date"),
    _curated("stg_jellyfin_activity", DOMAIN_MEDIA, "jellyfin", "activity",
             "Activity log entries, typed.", "partition_date"),
    _curated("stg_jellyfin_users", DOMAIN_MEDIA, "jellyfin", "users",
             "Account snapshots, typed.", "snapshot_date"),
    _curated("stg_jellyfin_library_items", DOMAIN_MEDIA, "jellyfin", "library_items",
             "Series, seasons, episodes, box sets and other videos, typed, with "
             "season_number/episode_number resolved from the raw index numbers.",
             "snapshot_date"),
    _curated("stg_jellyfin_searches", DOMAIN_MEDIA, "jellyfin", "searches",
             "Search requests, typed.", "partition_date"),
    _curated("stg_jellyfin_search_results", DOMAIN_MEDIA, "jellyfin", "search_results",
             "Search results, typed.", "partition_date"),
    _curated("stg_tmdb_titles", DOMAIN_MEDIA, "tmdb", "titles",
             "TMDB enrichment, typed, with the revenue-to-budget ratio derived.",
             None),
    _curated("stg_ai_platform_usage_events", DOMAIN_AI, "ai_platform", "usage_events",
             "Every api.ai gateway call, typed, full prompt and response included.",
             "partition_date"),
    _curated("stg_ai_platform_applications", DOMAIN_AI, "ai_platform", "applications",
             "The application registry, deduped to the newest snapshot per id.", None),
    _curated("stg_ai_platform_api_keys", DOMAIN_AI, "ai_platform", "api_keys",
             "The API key registry, deduped to the newest snapshot per id.", None),
    _curated("stg_ai_platform_agents", DOMAIN_AI, "ai_platform", "agents",
             "The agent registry, deduped to the newest snapshot per id.", None),
    _curated("stg_ai_platform_automations", DOMAIN_AI, "ai_platform", "automations",
             "The automation registry, deduped to the newest snapshot per id.", None),
    _curated("stg_ai_platform_users", DOMAIN_AI, "ai_platform", "users",
             "The user registry, deduped to the newest snapshot per id.", None),
    _curated("stg_ai_platform_runs", DOMAIN_AI, "ai_platform", "runs",
             "Every agent run, typed.", "partition_date"),
    _curated("stg_ai_platform_conversations", DOMAIN_AI, "ai_platform", "conversations",
             "Every conversation, typed.", "partition_date"),
    _curated("stg_ai_platform_run_steps", DOMAIN_AI, "ai_platform", "run_steps",
             "Every step of every run, typed.", "partition_date"),
    _curated("stg_ai_platform_messages", DOMAIN_AI, "ai_platform", "messages",
             "Every message, typed, full text included.", "partition_date"),
    _curated("stg_ai_platform_email_sends", DOMAIN_AI, "ai_platform", "email_sends",
             "Every email sent or attempted, typed.", "partition_date"),
    _curated("stg_ai_platform_automation_runs", DOMAIN_AI, "ai_platform", "automation_runs",
             "One row per run of an automation, typed.", "partition_date"),
    _curated("stg_ai_platform_automation_step_runs", DOMAIN_AI, "ai_platform",
             "automation_step_runs", "One row per step of one automation run, typed.",
             "partition_date"),
)

# --------------------------------------------------------------------------------
# marts: written by homelab_datalake.assets.publish, from the dbt marts
# --------------------------------------------------------------------------------
#
# No `source` segment. A mart is not owned by one source -- fct_library_title joins
# Jellyfin to TMDB -- so naming one of them in the path would be wrong for the
# reader half the time.

MART_DATASETS: tuple[Dataset, ...] = (
    _mart("fct_http_request", DOMAIN_INFRASTRUCTURE, "fct_http_request",
          "One row per request, with a stable key.", "partition_date"),
    _mart("agg_traffic_daily", DOMAIN_INFRASTRUCTURE, "agg_traffic_daily",
          "Requests, visitors and error rates by day, host and client kind.",
          "partition_date"),
    _mart("fct_llm_call", DOMAIN_AI, "fct_llm_call",
          "One row per model call, with cost and latency.", "partition_date"),
    _mart("agg_llm_spend_daily", DOMAIN_AI, "agg_llm_spend_daily",
          "Spend, calls and tokens by day, model, provider and workload.",
          "partition_date"),
    _mart("fct_camera_event", DOMAIN_MONITORING, "fct_camera_event",
          "One row per camera event, with time-of-day dimensions.", "partition_date"),
    _mart("agg_camera_activity_daily", DOMAIN_MONITORING, "agg_camera_activity_daily",
          "Events and outcomes by day, camera, zone and rule.", "partition_date"),
    _mart("fct_camera_clip", DOMAIN_MONITORING, "fct_camera_clip",
          "One row per clip, with the event that caused it.", "partition_date"),
    _mart("fct_camera_coverage", DOMAIN_MONITORING, "fct_camera_coverage",
          "One row per span where a zone could not be read.", "partition_date"),
    _mart("dim_camera_zone", DOMAIN_MONITORING, "dim_camera_zone",
          "The zone and rule lookup an event points at.", None),
    _mart("fct_library_title", DOMAIN_MEDIA, "fct_library_title",
          "One row per film per snapshot, Jellyfin joined to TMDB.", "snapshot_date"),
    _mart("dim_title_scd2", DOMAIN_MEDIA, "dim_title_scd2",
          "Title history: one row per version, with validity dates.", None),
    # Deliberately not versioned like dim_title_scd2 -- see fct_library_item.sql for
    # why a snapshot fact is the honest match for five very different item grains.
    _mart("fct_library_item", DOMAIN_MEDIA, "fct_library_item",
          "One row per series, season, episode, box set or other video per "
          "snapshot -- everything in the library that is not a movie.",
          "snapshot_date"),
    _mart("dim_genre", DOMAIN_MEDIA, "dim_genre", "The genre lookup.", None),
    _mart("bridge_title_genre", DOMAIN_MEDIA, "bridge_title_genre",
          "The many-to-many between titles and genres.", "snapshot_date"),
    _mart("fct_jellyfin_playback", DOMAIN_MEDIA, "fct_jellyfin_playback",
          "One row per playback session, joined to the title dimension where it "
          "resolves.", "partition_date"),
    _mart("fct_jellyfin_activity", DOMAIN_MEDIA, "fct_jellyfin_activity",
          "One row per server activity log entry.", "partition_date"),
    _mart("dim_jellyfin_user", DOMAIN_MEDIA, "dim_jellyfin_user",
          "The account each play and activity entry points at: latest snapshot "
          "only.", None),
    _mart("fct_jellyfin_search", DOMAIN_MEDIA, "fct_jellyfin_search",
          "One row per search request logged by the AI Search plugin.",
          "partition_date"),
    _mart("fct_jellyfin_search_result", DOMAIN_MEDIA, "fct_jellyfin_search_result",
          "One row per result a logged search returned.", "partition_date"),
    _mart("fct_ai_gateway_call", DOMAIN_AI, "fct_ai_gateway_call",
          "One row per api.ai gateway call: application, key, person, requested "
          "and actual model, full prompt and response, tokens, cost, latency.",
          "partition_date"),
    _mart("fct_agent_run", DOMAIN_AI, "fct_agent_run",
          "One row per agent run, whether or not it called the gateway.",
          "partition_date"),
    _mart("fct_agent_tool_call", DOMAIN_AI, "fct_agent_tool_call",
          "One row per tool call a run made, from run_steps.", "partition_date"),
    _mart("fct_conversation_message", DOMAIN_AI, "fct_conversation_message",
          "One row per message, full text included.", "partition_date"),
    _mart("fct_email_send", DOMAIN_AI, "fct_email_send",
          "One row per email the platform has sent or attempted to send.",
          "partition_date"),
    _mart("fct_automation_run", DOMAIN_AI, "fct_automation_run",
          "One row per automation run, with its step runs folded in.",
          "partition_date"),
)

# --------------------------------------------------------------------------------
# raw, added 2026-09-15: metrics, the media request pipeline, host logs, sign-ins
# --------------------------------------------------------------------------------
#
# Appended here rather than inside RAW_DATASETS above, so the sources other branches
# add there merge without touching the same lines. See docs/METRICS_STREAMING_PLAN.md.

_SAMPLES_ARE_TOO_MANY = (
    "~60 M samples a day; the warehouse is rebuilt in full from the lake at least once "
    "a day. Queried from the lake through the SQL API."
)

_STREAMED_RAW: tuple[Dataset, ...] = (
    _raw("prometheus_samples", DOMAIN_INFRASTRUCTURE, "prometheus", "samples",
         "Every raw sample Prometheus scraped: timestamp, series id, metric, job, instance, value.",
         lake_only=_SAMPLES_ARE_TOO_MANY),
    _raw("prometheus_series", DOMAIN_INFRASTRUCTURE, "prometheus", "series",
         "The label set behind each series id, per day it was seen.",
         lake_only="Only meaningful beside prometheus_samples, which is lake-only."),
    _raw("prometheus_metadata", DOMAIN_INFRASTRUCTURE, "prometheus", "metadata",
         "What each metric is: counter, gauge, histogram or summary, with help and unit.",
         lake_only="Only meaningful beside prometheus_samples, which is lake-only."),
    _raw("status_page_metric_samples", DOMAIN_INFRASTRUCTURE, "status_page", "metric_samples",
         "Chateau's and Ferme's cpu, memory and disk, every five minutes, as Sentinel reported them."),
    _raw("status_page_observations", DOMAIN_INFRASTRUCTURE, "status_page", "observations",
         "Every UP/DOWN/PENDING reading of each monitored server, about every 30 seconds."),
    _raw("status_page_incidents", DOMAIN_INFRASTRUCTURE, "status_page", "incidents",
         "Outage incidents, every version from investigating to resolved.", partition_column=None),
    _raw("status_page_transitions", DOMAIN_INFRASTRUCTURE, "status_page", "transitions",
         "Each change of a server's Sentinel status.", partition_column=None),
    _raw("status_page_services", DOMAIN_INFRASTRUCTURE, "status_page", "services",
         "The monitored servers and their last known status.", partition_column=None),
    _raw("host_journal", DOMAIN_INFRASTRUCTURE, "host", "journal",
         "fraserv's systemd journal: every entry, every field.",
         lake_only="~220 k entries a day, most of them CasaOS chatter; rebuilt nightly it would dominate the warehouse."),
    _raw("host_auth_log", DOMAIN_INFRASTRUCTURE, "host", "auth_log",
         "fraserv's auth.log: SSH sign-ins, sudo, cron sessions."),
    _raw("keycloak_events", DOMAIN_INFRASTRUCTURE, "keycloak", "events",
         "Every Keycloak user event: logins, logouts, token exchanges and their errors."),
    _raw("keycloak_admin_events", DOMAIN_INFRASTRUCTURE, "keycloak", "admin_events",
         "Every change made through Keycloak's admin API."),
    _raw("keycloak_users", DOMAIN_INFRASTRUCTURE, "keycloak", "users",
         "Keycloak accounts, every version. No credentials.", partition_column=None),
    _raw("keycloak_clients", DOMAIN_INFRASTRUCTURE, "keycloak", "clients",
         "The applications that sign in through Keycloak. No secrets.", partition_column=None),
    _raw("keycloak_realms", DOMAIN_INFRASTRUCTURE, "keycloak", "realms",
         "Realms and their event settings.", partition_column=None),
    *(
        _raw(spec.table_name, DOMAIN_MEDIA, spec.app, spec.name, spec.description, partition_column=None)
        for spec in _media_pipeline.ALL_TABLES
    ),
)

RAW_DATASETS = RAW_DATASETS + _STREAMED_RAW

# --------------------------------------------------------------------------------
# finance, added 2026-09-29: the banking tracker's ledger (assets/finance.py)
# --------------------------------------------------------------------------------
#
# Its own domain: nobody reads personal accounts beside proxy logs or model spend. Appended
# after the other sections for the same merge reason they are.

DOMAIN_FINANCE = "finance"

_FINANCE_RAW: tuple[Dataset, ...] = (
    _raw("finance_accounts", DOMAIN_FINANCE, "banking_tracker", "accounts",
         "Bank and manual accounts, every version.", partition_column=None),
    _raw("finance_transactions", DOMAIN_FINANCE, "banking_tracker", "transactions",
         ("Every transaction with its category, type and home-currency amount, every "
          "version: a recategorised or deleted transaction keeps its history."),
         partition_column=None),
    _raw("finance_recurring_series", DOMAIN_FINANCE, "banking_tracker", "recurring_series",
         "Subscriptions and bills the tracker detected, every version.", partition_column=None),
    _raw("finance_categories", DOMAIN_FINANCE, "banking_tracker", "categories",
         "The category catalogue and the picker group each belongs to.", partition_column=None),
    _raw("finance_account_balances", DOMAIN_FINANCE, "banking_tracker", "account_balances",
         "Each account's balance, in its own and the home currency, per day read."),
    # Every table the tracker has, every column, straight from its database: properties,
    # mortgages, holdings, trades, the bank metadata on each transaction, and the rest.
    *(
        _raw(_finance_tables.lake_table(name), DOMAIN_FINANCE, "banking_tracker_db",
             _finance_tables.snake(name),
             f"The tracker's {name} table, every column, the newest version of each row.",
             partition_column=None)
        for name in _finance_tables.TABLES
    ),
)

_FINANCE_MARTS: tuple[Dataset, ...] = (
    _mart("fct_finance_transaction", DOMAIN_FINANCE, "fct_finance_transaction",
          "One row per current transaction, with account, category group and type.", None),
    _mart("fct_finance_account_balance", DOMAIN_FINANCE, "fct_finance_account_balance",
          "Each account's balance per day, in the home currency.", None),
    _mart("dim_finance_recurring_series", DOMAIN_FINANCE, "dim_finance_recurring_series",
          "Current subscriptions and bills, active or ended.", None),
)

RAW_DATASETS = RAW_DATASETS + _FINANCE_RAW
MART_DATASETS = MART_DATASETS + _FINANCE_MARTS

ALL_DATASETS: tuple[Dataset, ...] = RAW_DATASETS + CURATED_DATASETS + MART_DATASETS

_BY_TABLE = {d.table: d for d in ALL_DATASETS}


def dataset(table: str) -> Dataset:
    try:
        return _BY_TABLE[table]
    except KeyError:
        raise KeyError(
            f"no dataset registered for table {table!r}. Add it to "
            f"homelab_datalake.layout; known: {sorted(_BY_TABLE)}"
        ) from None


def prefix(table: str) -> str:
    """The lake prefix for a table name. The only supported way to build one."""
    return dataset(table).prefix


def datasets_in(layer: str) -> tuple[Dataset, ...]:
    return tuple(d for d in ALL_DATASETS if d.layer == layer)


def migrations() -> tuple[tuple[str, str], ...]:
    """(old prefix, new prefix) for every dataset that has moved.

    Read by scripts/migrate_lake_layout.py. Built from the registry rather than
    written out again there, so the two cannot disagree.
    """
    return tuple(
        (d.legacy_prefix, d.prefix)
        for d in ALL_DATASETS
        if d.legacy_prefix and d.legacy_prefix != d.prefix
    )
