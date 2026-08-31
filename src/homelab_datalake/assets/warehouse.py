"""Loading the lake into DuckDB, and publishing what the serving layer reads.

The DuckDB file is a build artefact, not a source of truth. If it is lost, this
asset plus `dbt build` reconstructs it from the Parquet in the lake. That is what
sets the backup policy: the bucket is backed up, the warehouse is not.

Each raw table is its own asset rather than one asset called "the warehouse". That
is not decoration: dbt already declares `source('raw', 'camera_events')` in YAML, and
naming the same thing on the Dagster side is what joins the two graphs. The lineage
then runs unbroken from a Parquet partition, through the raw table, into staging and
out to a mart -- so "what feeds this number" is a question the UI answers, rather
than one somebody answers by reading two projects side by side.
"""

import os
from pathlib import Path

import duckdb
import pyarrow as pa
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)

from homelab_datalake import serving
from homelab_datalake.assets.camera import (
    _ANNOTATION_SCHEMA,
    _CLIP_SCHEMA,
    _EVENT_SCHEMA,
    _LOG_SCHEMA,
    _OBSERVABILITY_SCHEMA,
    _ZONE_SCHEMA,
    ANNOTATION_PREFIX,
    CLIP_PREFIX,
    EVENT_PREFIX,
    LOG_PREFIX,
    OBSERVABILITY_PREFIX,
    ZONE_PREFIX,
    camera_annotations_raw,
    camera_events_raw,
    camera_observability_raw,
    camera_zones_raw,
)
from homelab_datalake.assets.dbt_marts import dbt_marts
from homelab_datalake.assets.jellyfin import (
    _GENRE_SCHEMA,
    _PEOPLE_SCHEMA,
    _STREAM_SCHEMA,
    _TITLE_SCHEMA,
    GENRE_PREFIX,
    PEOPLE_PREFIX,
    STREAM_PREFIX,
    TITLE_PREFIX,
    jellyfin_library_raw,
)
from homelab_datalake.assets.litellm_spend import _SCHEMA as SPEND_SCHEMA
from homelab_datalake.assets.litellm_spend import SPEND_PREFIX, litellm_spend_raw
from homelab_datalake.assets.npm_access import (
    _REJECT_SCHEMA,
    RAW_PREFIX,
    REJECT_PREFIX,
    npm_access_raw,
)
from homelab_datalake.assets.npm_access import _SCHEMA as ACCESS_SCHEMA
from homelab_datalake.assets.tmdb import _SCHEMA as _TMDB_SCHEMA
from homelab_datalake.assets.tmdb import TMDB_PREFIX, tmdb_titles_raw
from homelab_datalake.resources import LakeStorage

# Resolved to an absolute path once, here, and written back to the environment.
#
# dbt reads DUCKDB_PATH out of the environment itself, and dagster-dbt runs it with
# the dbt project as its working directory -- so a relative path means Dagster opens
# ./warehouse/datalake.duckdb and dbt opens ./dbt/warehouse/datalake.duckdb. Two
# files, no error: dbt cheerfully creates its own empty one and then fails to find a
# source table, which reads as a broken model rather than a path problem. Normalising
# once removes the possibility rather than documenting it.
DUCKDB_PATH = str(Path(os.environ.get("DUCKDB_PATH", "warehouse/datalake.duckdb")).resolve())
os.environ["DUCKDB_PATH"] = DUCKDB_PATH

# The asset key for a dbt source is ["raw", <table>], so these keys are what connect
# the two graphs. Change one and the dbt models silently detach from what feeds them,
# which is why the names live in one list rather than being spelled out twice.
_SOURCES = (
    ("npm_access", RAW_PREFIX, ACCESS_SCHEMA, npm_access_raw,
     "One row per HTTP request seen by an Nginx Proxy Manager instance."),
    ("npm_access_rejects", REJECT_PREFIX, _REJECT_SCHEMA, npm_access_raw,
     ("Lines no log format matched, kept with their text. The reject rate is a "
      "data-quality signal, so these are never dropped.")),
    ("litellm_spend", SPEND_PREFIX, SPEND_SCHEMA, litellm_spend_raw,
     "One row per LLM call. Prompt and completion payloads are never selected."),
    ("jellyfin_titles", TITLE_PREFIX, _TITLE_SCHEMA, jellyfin_library_raw,
     "One row per film per snapshot date. No paths, no playback history."),
    ("jellyfin_title_genres", GENRE_PREFIX, _GENRE_SCHEMA, jellyfin_library_raw,
     "Title-to-genre pairs, kept separate because a title has many genres."),
    ("jellyfin_title_streams", STREAM_PREFIX, _STREAM_SCHEMA, jellyfin_library_raw,
     "One row per media stream: every audio, video and subtitle track."),
    ("jellyfin_title_people", PEOPLE_PREFIX, _PEOPLE_SCHEMA, jellyfin_library_raw,
     "Cast and crew credits. Public film metadata, not library users."),
    ("camera_events", EVENT_PREFIX, _EVENT_SCHEMA, camera_events_raw,
     "Attention events. Not final when first written: vision analysis lands later."),
    ("camera_clips", CLIP_PREFIX, _CLIP_SCHEMA, camera_events_raw,
     "Clip metadata. No paths, playlists or thumbnails."),
    ("camera_logs", LOG_PREFIX, _LOG_SCHEMA, camera_events_raw,
     "Activity log entries, with the free text people wrote in them."),
    ("camera_zones", ZONE_PREFIX, _ZONE_SCHEMA, camera_zones_raw,
     "Zone and rule configuration per day, geometry included."),
    ("camera_observability", OBSERVABILITY_PREFIX, _OBSERVABILITY_SCHEMA,
     camera_observability_raw,
     "Spans where a zone could not be read, with the reason it could not."),
    ("camera_annotations", ANNOTATION_PREFIX, _ANNOTATION_SCHEMA, camera_annotations_raw,
     "Notes and marks left against a clip or an event."),
    ("tmdb_titles", TMDB_PREFIX, _TMDB_SCHEMA, tmdb_titles_raw,
     ("Budget, revenue, language and vote counts by TMDB id. Empty when no API key "
      "is configured, which leaves the library marts' enrichment columns null.")),
)


# DuckDB types for the Arrow types these schemas use. Deliberately a small explicit
# map rather than a generic converter: an unmapped type should fail loudly at build
# time, not land in the warehouse as something surprising.
_DUCKDB_TYPES = {
    pa.date32(): "DATE",
    pa.string(): "VARCHAR",
    pa.int32(): "INTEGER",
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.bool_(): "BOOLEAN",
    pa.timestamp("us", tz="UTC"): "TIMESTAMP WITH TIME ZONE",
}


def empty_table_ddl(table: str, schema: pa.Schema) -> str:
    """CREATE TABLE for a source with no landed partitions yet.

    An empty table with the *right columns* rather than a placeholder, because dbt
    reads column names at compile time: a stand-in table fails the whole build with a
    binder error that says nothing about the real cause, which is simply that nothing
    has landed. This happens on a fresh deployment and in CI, where the LiteLLM source
    has no database to read.
    """
    columns = ", ".join(f'"{f.name}" {_duckdb_type(f.type)}' for f in schema)
    return f"CREATE OR REPLACE TABLE {table} ({columns})"


def _duckdb_type(dtype: pa.DataType) -> str:
    """One Arrow type as DuckDB spells it, structs and lists included.

    The scalar map stays explicit so an unmapped type fails loudly rather than landing
    as something surprising. Structs and lists are built recursively from it, because
    writing `STRUCT(person BIGINT, ...)` out by hand in a second place is how the
    declared shape and the real one drift apart -- which is the failure this whole
    module exists to prevent.
    """
    if pa.types.is_struct(dtype):
        fields = ", ".join(f'"{f.name}" {_duckdb_type(f.type)}' for f in dtype)
        return f"STRUCT({fields})"
    if pa.types.is_list(dtype):
        return f"{_duckdb_type(dtype.value_type)}[]"
    return _DUCKDB_TYPES[dtype]


def connect() -> duckdb.DuckDBPyConnection:
    """Open the warehouse with its session timezone pinned to UTC.

    Casting a TIMESTAMP WITH TIME ZONE to a date renders it in the session
    timezone, so without this a request logged at 23:30 UTC lands on the next day
    on a Europe/Paris machine. Rows would then be assigned to different partitions
    depending on who ran the job, which breaks idempotency. dbt pins the same
    setting in profiles.yml; both connections must agree.
    """
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("SET TimeZone='UTC';")
    return con


@multi_asset(
    specs=[
        AssetSpec(
            key=AssetKey(["raw", name]),
            deps=[upstream],
            group_name="warehouse",
            description=description,
            kinds={"duckdb"},
        )
        for name, _prefix, _schema, upstream, description in _SOURCES
    ],
    # Not subsettable: the whole point is one full rebuild from the lake, so there is
    # no cheaper path through this that materialising a single table would take.
    can_subset=False,
)
def warehouse_tables(context: AssetExecutionContext, lake: LakeStorage):
    """Every landed partition, read from the lake into DuckDB under `raw`.

    Rebuilt in full from Parquet on every run, so the warehouse cannot drift from
    object storage: it holds what the lake holds, or the run fails.
    """
    with connect() as con:
        lake.configure_duckdb(con)
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        counts: dict[str, int] = {}
        for name, prefix, schema, _upstream, _description in _SOURCES:
            pattern = lake.duckdb_scan_target(prefix)
            table = f"raw.{name}"
            # The table names are module constants, never input. The glob is built
            # from configuration -- bucket name, local root -- so it is escaped
            # rather than trusted: a stray quote in an env var would otherwise end
            # the string literal and change the statement.
            safe_pattern = pattern.replace("'", "''")
            try:
                # union_by_name tolerates a partition written before a column
                # existed, which is the schema-evolution path.
                con.execute(
                    f"CREATE OR REPLACE TABLE {table} AS "  # noqa: S608 - literal identifier
                    f"SELECT * FROM read_parquet('{safe_pattern}', union_by_name=true)"
                )
            except duckdb.IOException:
                # No partitions landed yet. An empty table is the honest state --
                # better than a missing one, which would fail dbt with a confusing error.
                context.log.warning(
                    "no objects under %s; creating %s empty with its declared schema",
                    pattern, table,
                )
                con.execute(empty_table_ddl(table, schema))
            counts[name] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608

        partitions = (
            con.execute("SELECT count(DISTINCT (dt, host_source)) FROM raw.npm_access").fetchone()[0]
            if counts["npm_access"]
            else 0
        )

    for name, prefix, _schema, _upstream, _description in _SOURCES:
        metadata = {
            "rows": counts[name],
            "source": MetadataValue.path(lake.duckdb_scan_target(prefix)),
            "warehouse": MetadataValue.path(DUCKDB_PATH),
        }
        if name == "npm_access":
            metadata["partitions_loaded"] = partitions
        yield MaterializeResult(asset_key=AssetKey(["raw", name]), metadata=metadata)


@asset(
    deps=[dbt_marts],
    group_name="warehouse",
    compute_kind="duckdb",
    description=(
        "Publishes the marts to a separate read-only file for the MCP layer. Depends "
        "on the dbt models, so a failed build never reaches readers -- they keep "
        "serving the previous copy rather than half a rebuild."
    ),
)
def warehouse_serving(context: AssetExecutionContext) -> Output[dict]:
    published = serving.publish(DUCKDB_PATH)
    context.log.info(
        "published %d tables, %d rows, %.1f MB to %s",
        len(published.tables), published.rows, published.bytes_written / 1e6, published.path,
    )
    return Output(
        {"tables": published.tables, "rows": published.rows},
        metadata={
            "tables": len(published.tables),
            "rows": published.rows,
            "size_mb": round(published.bytes_written / 1e6, 2),
            "path": MetadataValue.path(published.path),
            "published": MetadataValue.text(", ".join(published.tables)),
        },
    )
