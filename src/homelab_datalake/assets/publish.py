"""Writing the derived layers back into the lake as Parquet.

Without this the bucket holds only what was ingested, and every cleaned or
aggregated number lives in a DuckDB file that one process opens at a time. That is
enough for the MCP server, and not enough for anything else: a notebook, a BI tool
or another person cannot read it while the nightly build holds the write lock.

So the layers dbt produces are published back to object storage, where any engine
that reads Parquet can open them, in parallel, with no lock and no server:

    curated/  the cleaned and enriched records, one dataset per source
    marts/    the facts, dimensions and daily aggregates

Which tables go where, and which column they are partitioned on, is declared once in
homelab_datalake.layout. This module only moves the rows.

Two properties are worth naming, because they are why this is written the way it is.

**A published partition is replaced, never appended to.** Every partition is written
first, and only then is anything left over from a previous run removed. There is no
moment when the prefix is empty, so a reader that globs it during a rebuild sees the
previous partition or the new one, never nothing.

**The layer is rebuilt from the warehouse, not accumulated.** If a dbt model changes,
the next run republishes every partition it touches. The lake cannot drift into
holding a column definition nobody uses any more.
"""

# No `from __future__ import annotations` here. Dagster resolves the `context`
# annotation at decoration time to decide which context type an asset wants, and a
# string annotation makes that check fail with a message that says the type is wrong
# when the type is right.
from dataclasses import dataclass
from pathlib import Path

import duckdb
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    multi_asset,
)

from homelab_datalake import layout
from homelab_datalake.assets.warehouse import DUCKDB_PATH
from homelab_datalake.resources import LakeStorage


@dataclass
class PublishedDataset:
    table: str
    prefix: str
    rows: int
    objects: int
    bytes_written: int
    removed: int


def resolve_schema(con: duckdb.DuckDBPyConnection, table: str) -> str:
    """The schema a dbt model actually landed in.

    dbt-duckdb prefixes the configured schema with the target's, so `marts` is
    really `main_marts`. Looking the table up in the catalogue rather than
    assembling the name means a target rename does not turn into a confusing
    "table not found" halfway through a publish.
    """
    rows = con.execute(
        "SELECT schema_name FROM duckdb_tables() "
        "WHERE table_name = ? AND database_name = current_database() "
        "UNION ALL SELECT schema_name FROM duckdb_views() "
        "WHERE view_name = ? AND database_name = current_database()",
        [table, table],
    ).fetchall()
    if not rows:
        raise RuntimeError(
            f"{table!r} is not in the warehouse. It is declared in "
            "homelab_datalake.layout, so either the dbt model was renamed or the "
            "build did not run."
        )
    return rows[0][0]


def _partition_values(con, schema: str, table: str, column: str) -> list:
    return [
        row[0]
        for row in con.execute(
            f'SELECT DISTINCT "{column}" FROM "{schema}"."{table}" '  # noqa: S608
            f'WHERE "{column}" IS NOT NULL ORDER BY 1'
        ).fetchall()
    ]


def publish_dataset(con, lake: LakeStorage, dataset: layout.Dataset) -> PublishedDataset:
    """Write one dataset to its prefix, then remove what a previous run left."""
    schema = resolve_schema(con, dataset.table)
    quoted = f'"{schema}"."{dataset.table}"'
    written: dict[str, int] = {}
    rows = 0

    if dataset.partition_column is None:
        table = con.execute(f"SELECT * FROM {quoted}").to_arrow_table()  # noqa: S608
        key = f"{dataset.prefix}/data.parquet"
        written[key] = lake.write_table(key, table)
        rows = table.num_rows
    else:
        column = dataset.partition_column
        for value in _partition_values(con, schema, dataset.table, column):
            # The value is a DATE from the catalogue, never a string from a caller,
            # and it is passed as a parameter rather than interpolated anyway.
            table = con.execute(
                f'SELECT * FROM {quoted} WHERE "{column}" = ?', [value]  # noqa: S608
            ).to_arrow_table()
            key = f"{dataset.prefix}/{column}={value.isoformat()}/data.parquet"
            written[key] = lake.write_table(key, table)
            rows += table.num_rows

    removed = _remove_stale(lake, dataset.prefix, set(written))
    return PublishedDataset(
        table=dataset.table,
        prefix=dataset.prefix,
        rows=rows,
        objects=len(written),
        bytes_written=sum(written.values()),
        removed=removed,
    )


def _remove_stale(lake: LakeStorage, prefix: str, keep: set[str]) -> int:
    """Delete objects under a prefix that this run did not write.

    This is what makes a republish a replacement. It runs after the writes, not
    before, so the prefix is never empty -- and it only ever considers keys under
    the one prefix it was given.
    """
    removed = 0
    if lake.uses_s3:
        client = lake._client()
        stale = []
        pages = client.get_paginator("list_objects_v2")
        for page in pages.paginate(Bucket=lake.bucket, Prefix=prefix + "/"):
            stale += [
                {"Key": obj["Key"]}
                for obj in page.get("Contents", [])
                if obj["Key"] not in keep
            ]
        for batch in (stale[i:i + 1000] for i in range(0, len(stale), 1000)):
            client.delete_objects(Bucket=lake.bucket, Delete={"Objects": batch})
            removed += len(batch)
    else:
        root = Path(lake.local_root)
        for path in (root / prefix).rglob("*.parquet"):
            if str(path.relative_to(root)) not in keep:
                path.unlink()
                removed += 1
        # S3 has no directories, so on that backend removing the object is the whole
        # job. On disk an emptied partition directory would stay, and `ls` would
        # then report a partition that holds nothing -- so it goes too.
        for directory in sorted((root / prefix).rglob("*"), reverse=True):
            if directory.is_dir() and not any(directory.iterdir()):
                directory.rmdir()
    return removed


# The dbt model behind each dataset, so lineage runs unbroken from a landed
# partition to the object published back to the lake. Derived from the name rather
# than declared again: dbt's own folder layout already enforces the prefixes, and a
# model that got neither prefix is a mart by construction.
_MODEL_LAYERS = (("stg_", "staging"), ("int_", "intermediate"))


def dbt_asset_key(table: str) -> AssetKey:
    for prefix, group in _MODEL_LAYERS:
        if table.startswith(prefix):
            return AssetKey([group, table])
    return AssetKey(["marts", table])


def read_only_warehouse() -> duckdb.DuckDBPyConnection:
    """The warehouse, attached read-only, without taking its write lock.

    Publishing reads; it never writes to DuckDB. Opening the file read-write would
    take the single write lock and make this step race the one that publishes the
    serving copy -- which is exactly what happened the first time this ran: two
    steps failed with "Conflicting lock is held" while the third did the work.
    Several read-only handles on one file coexist.

    The alias is the file's own name rather than something readable like
    "warehouse", and that is load-bearing. Half of what is published here are dbt
    *views*, and DuckDB stores a view's SQL with its catalogue qualified -- the
    stored text says `datalake.main_staging.stg_npm_access`. Attached under any other
    name, every one of those views fails to bind with `Catalog "datalake" does not
    exist`, which is what the second production run did. Attaching under the name the
    view definitions already use makes them resolve.
    """
    alias = Path(DUCKDB_PATH).stem
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC';")
    con.execute(
        f"ATTACH '{DUCKDB_PATH.replace(chr(39), chr(39) * 2)}' AS \"{alias}\" (READ_ONLY);"
    )
    con.execute(f'USE "{alias}";')
    return con


def _publish(context: AssetExecutionContext, lake: LakeStorage, layer: str):
    with read_only_warehouse() as con:
        for dataset in layout.datasets_in(layer):
            result = publish_dataset(con, lake, dataset)
            context.log.info(
                "%s -> %s: %d rows in %d objects, %.2f MB, %d stale removed",
                result.table, result.prefix, result.rows, result.objects,
                result.bytes_written / 1e6, result.removed,
            )
            yield MaterializeResult(
                asset_key=AssetKey(["lake", layer, dataset.table]),
                metadata={
                    "rows": result.rows,
                    "objects": result.objects,
                    "bytes": result.bytes_written,
                    "prefix": MetadataValue.path(f"{lake.describe()}/{result.prefix}"),
                    "domain": dataset.domain,
                    "partitioned_by": dataset.partition_column or "not partitioned",
                },
            )


def _specs(layer: str) -> list[AssetSpec]:
    return [
        AssetSpec(
            key=AssetKey(["lake", layer, dataset.table]),
            # The specific model, not the whole dbt build. This is what makes the
            # UI answer "what feeds this object" with one edge instead of eighteen.
            deps=[dbt_asset_key(dataset.table)],
            group_name="warehouse",
            description=dataset.description,
            kinds={"parquet"},
            metadata={"prefix": dataset.prefix, "domain": dataset.domain},
        )
        for dataset in layout.datasets_in(layer)
    ]


@multi_asset(specs=_specs(layout.LAYER_CURATED), can_subset=False)
def lake_curated(context: AssetExecutionContext, lake: LakeStorage):
    """The cleaned and enriched records, published to `curated/` in the lake."""
    yield from _publish(context, lake, layout.LAYER_CURATED)


@multi_asset(specs=_specs(layout.LAYER_MARTS), can_subset=False)
def lake_marts(context: AssetExecutionContext, lake: LakeStorage):
    """The facts, dimensions and aggregates, published to `marts/` in the lake."""
    yield from _publish(context, lake, layout.LAYER_MARTS)


# Named so the warehouse module's own use of DUCKDB_PATH stays the single source of
# the path. Imported here only to keep the reference from being flagged as unused;
# the connection itself comes from warehouse.connect().
_ = DUCKDB_PATH
