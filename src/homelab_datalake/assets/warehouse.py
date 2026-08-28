"""Loading the lake into DuckDB, and running dbt on top of it.

The DuckDB file is a build artefact, not a source of truth. If it is lost, this
asset plus `dbt build` reconstructs it from the Parquet in the lake. That is what
sets the backup policy: the bucket is backed up, the warehouse is not.
"""

import os
from pathlib import Path

import duckdb
import pyarrow as pa
from dagster import AssetExecutionContext, MetadataValue, Output, asset

from homelab_datalake.assets.litellm_spend import _SCHEMA as SPEND_SCHEMA
from homelab_datalake.assets.litellm_spend import SPEND_PREFIX, litellm_spend_raw
from homelab_datalake.assets.npm_access import (
    _REJECT_SCHEMA,
    RAW_PREFIX,
    REJECT_PREFIX,
    npm_access_raw,
)
from homelab_datalake.assets.npm_access import _SCHEMA as ACCESS_SCHEMA
from homelab_datalake.resources import LakeStorage

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "warehouse/datalake.duckdb")


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
    columns = ", ".join(f'"{f.name}" {_DUCKDB_TYPES[f.type]}' for f in schema)
    return f"CREATE OR REPLACE TABLE {table} ({columns})"


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


@asset(
    deps=[npm_access_raw, litellm_spend_raw],
    group_name="warehouse",
    compute_kind="duckdb",
    description=(
        "Every landed partition, read from the lake into DuckDB under `raw`. Rebuilt in "
        "full from Parquet each run, so the warehouse can never drift from the lake."
    ),
)
def npm_access_warehouse(context: AssetExecutionContext, lake: LakeStorage) -> Output[dict]:
    tables = {
        "raw.npm_access": (lake.duckdb_scan_target(RAW_PREFIX), ACCESS_SCHEMA),
        "raw.npm_access_rejects": (lake.duckdb_scan_target(REJECT_PREFIX), _REJECT_SCHEMA),
        "raw.litellm_spend": (lake.duckdb_scan_target(SPEND_PREFIX), SPEND_SCHEMA),
    }
    data_glob = tables["raw.npm_access"][0]

    with connect() as con:
        lake.configure_duckdb(con)
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        # A full rebuild rather than an incremental merge. At this volume it costs
        # seconds, and it removes a whole class of bug: the warehouse cannot hold a
        # row the lake no longer has. union_by_name tolerates a partition written
        # before a column existed, which is the schema-evolution path.
        for table, (pattern, schema) in tables.items():
            # The table names are module constants, never input. The glob is built
            # from configuration -- bucket name, local root -- so it is escaped
            # rather than trusted: a stray quote in an env var would otherwise end
            # the string literal and change the statement.
            safe_pattern = pattern.replace("'", "''")
            try:
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

        counts = {
            name: con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]  # noqa: S608
            for name in tables
        }
        partitions = (
            con.execute("SELECT count(DISTINCT (dt, host_source)) FROM raw.npm_access").fetchone()[0]
            if counts["raw.npm_access"]
            else 0
        )

    return Output(
        {"counts": counts, "partitions": partitions},
        metadata={
            "npm_rows": counts["raw.npm_access"],
            "npm_rejects": counts["raw.npm_access_rejects"],
            "litellm_calls": counts["raw.litellm_spend"],
            "npm_partitions_loaded": partitions,
            "source": MetadataValue.path(data_glob),
            "warehouse": MetadataValue.path(DUCKDB_PATH),
        },
    )
