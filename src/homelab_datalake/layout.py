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

LAYER_RAW = "raw"
LAYER_CURATED = "curated"
LAYER_MARTS = "marts"

LAYERS = (LAYER_RAW, LAYER_CURATED, LAYER_MARTS)

# Domains. A domain answers "who asks questions about this", not "which container
# produced it". Camera and reverse-proxy logs are both operational telemetry, but
# nobody looks at them together, so they are not one domain.
DOMAIN_INFRASTRUCTURE = "infrastructure"  # the edge: reverse proxy, request traffic
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


def _raw(table, domain, source, name, description, partition_column="dt"):
    return Dataset(
        table=table, layer=LAYER_RAW, domain=domain, source=source, name=name,
        description=description, partition_column=partition_column,
        legacy_prefix=f"raw/{table}",
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
    _raw("jellyfin_playback", DOMAIN_MEDIA, "jellyfin", "playback",
         "Every Playback Reporting plugin row: who watched what, on what, for how "
         "long the player was open.", partition_column="partition_hour"),
    _raw("jellyfin_activity", DOMAIN_MEDIA, "jellyfin", "activity",
         "The server activity log: sign-ins, playback starts and stops, library and "
         "account changes."),
    _raw("jellyfin_users", DOMAIN_MEDIA, "jellyfin", "users",
         "Daily snapshot of the account list: name, admin flag, disabled flag, last "
         "seen. Nothing from configuration or password policy beyond those."),
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
    _curated("stg_jellyfin_playback", DOMAIN_MEDIA, "jellyfin", "playback",
             "Playback sessions, typed, with the series name parsed out of episode "
             "titles.", "partition_date"),
    _curated("stg_jellyfin_activity", DOMAIN_MEDIA, "jellyfin", "activity",
             "Activity log entries, typed.", "partition_date"),
    _curated("stg_jellyfin_users", DOMAIN_MEDIA, "jellyfin", "users",
             "Account snapshots, typed.", "snapshot_date"),
    _curated("stg_tmdb_titles", DOMAIN_MEDIA, "tmdb", "titles",
             "TMDB enrichment, typed, with the revenue-to-budget ratio derived.",
             None),
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
)

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
