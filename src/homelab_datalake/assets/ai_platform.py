"""Landing the AI platform's own database, table by table, from one registry.

Fifty-two tables, one factory. `_make_table_asset` turns a single
`ai_platform_registry.TableSpec` into one partitioned Dagster asset; the module-level
list comprehension at the bottom calls it once per registry entry. Add a table to the
registry and it gets an asset, a lineage edge, a row-count check and (for
`usage_events`) a frame-count check, all without a line added here -- which is the
whole point of the registry existing at all. See `ai_platform_registry.py` for why
each table is `MODE_SNAPSHOT` or `MODE_WINDOW`, and for the seven-day trailing window.

**Object writes happen before the Parquet write, and stale ones are cleared first.**
A row's `object_key` or `frame_keys` are computed deterministically (see
`parsing.ai_platform`), so re-running a partition rewrites the same objects rather
than accumulating new ones beside old ones -- except when a re-run's row set has
*shrunk* (a usage_event lost a frame, a row was deleted upstream and no longer
appears in the window), in which case a stale object from the previous run would
otherwise survive under a key nothing points at any more. So the file/frame prefix
for the day is cleared before anything is written, then every current object is
written, and only then is the Parquet row written pointing at them -- a reader can
never see a `frame_keys` entry that does not resolve, because the Parquet write is
last.

**Concurrency.** Every extraction opens and closes exactly one Postgres connection
(see `parsing.ai_platform._fetch`); nothing here pools one. The `datalake_ro` role
carries `CONNECTION LIMIT 3` (`infra/ai-platform-datalake-role.sql`), which bounds
how many of these fifty-two per-table steps may genuinely be mid-query at once --
Dagster's own run/step concurrency configuration is what enforces that bound in
practice, and is an operational setting for whoever deploys this, not a line of
code that belongs here.
"""

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
from homelab_datalake.parsing import ai_platform as parsing
from homelab_datalake.parsing.ai_platform_registry import (
    MODE_SNAPSHOT,
    MODE_WINDOW,
    REGISTRY,
    TableSpec,
    registry_coverage,
)
from homelab_datalake.parsing.ai_platform_types import arrow_schema_for
from homelab_datalake.resources import LakeStorage

# usage_events since 2026-07-02 is the earliest row the owner measured this ledger
# holding ([count withheld] calls, [volume withheld], as of 2026-09-14); every other table in the registry
# either started at the same time as the platform or later, so backfilling from here
# produces empty partitions before a table existed rather than an error -- the same
# choice litellm_spend.py makes for SPEND_START.
AI_PLATFORM_START = "2026-07-02"

ai_platform_partitions = DailyPartitionsDefinition(start_date=AI_PLATFORM_START)

CHECK_ROW_COUNT = "extracted_rows_match_landed_rows"
CHECK_FRAME_COUNT = "frame_count_matches_objects_written"


def row_count_check_passed(extracted: int, landed: int) -> bool:
    """CHECK_ROW_COUNT's pass/fail rule, pulled out so it is testable on its own:
    what got extracted from ai_platform for a partition must equal what the landed
    Parquet object actually holds. A silent truncation or duplication in the write
    path would otherwise still look like a healthy green materialisation."""
    return extracted == landed


def frame_count_check_passed(frames_expected: int, objects_present: int) -> bool:
    """CHECK_FRAME_COUNT's pass/fail rule: every frame a row claims to have decoded
    (`sum(frame_count)`) must have an object actually sitting behind it. A mismatch
    means a frame was decoded but never written, or written and then orphaned by a
    partial re-run."""
    return frames_expected == objects_present


def _objects_prefix_for_day(spec: TableSpec, day: date) -> str | None:
    """Where this table's binary or frame objects live for one day, or None."""
    if spec.binary_column:
        return f"{parsing.BINARY_PREFIXES[spec.name]}/dt={day.isoformat()}"
    if spec.decode_metadata_images:
        return f"{parsing.GATEWAY_FRAMES_PREFIX}/dt={day.isoformat()}"
    return None


def _make_table_asset(spec: TableSpec):
    """One Dagster asset for one registry entry. See the module docstring."""
    schema = arrow_schema_for(spec)
    prefix = layout.prefix(spec.table_name)
    asset_name = f"{spec.table_name}_raw"

    check_specs = [
        AssetCheckSpec(
            name=CHECK_ROW_COUNT,
            asset=asset_name,
            description=(
                "The number of rows extracted from ai_platform for this partition "
                "equals the number of rows the landed Parquet object actually "
                "holds -- a write that silently truncated or duplicated would still "
                "look like a healthy green materialisation without this."
            ),
        )
    ]
    if spec.decode_metadata_images:
        check_specs.append(
            AssetCheckSpec(
                name=CHECK_FRAME_COUNT,
                asset=asset_name,
                description=(
                    "sum(frame_count) across the day's rows equals the number of "
                    "frame objects actually sitting under gateway_frames/dt=<day>. "
                    "A mismatch means a frame was decoded but never written, or "
                    "written and then orphaned by a partial re-run."
                ),
            )
        )

    window_note = (
        "Re-extracted on a trailing window every "
        "day -- see ai_platform_registry.WINDOW_DAYS -- so an update within that "
        "window (a run completing, an approval being decided) lands on its next "
        "re-run rather than being frozen at its first-seen values."
        if spec.mode == MODE_WINDOW else
        "Snapshotted whole, every day: small enough that re-reading it in full is "
        "simpler than reasoning about which rows changed."
    )

    @asset(
        name=asset_name,
        partitions_def=ai_platform_partitions,
        group_name="ai_platform",
        compute_kind="python",
        retry_policy=RetryPolicy(max_retries=3, delay=10),
        check_specs=check_specs,
        description=f"{spec.description} {window_note}",
    )
    def _table_asset(context: AssetExecutionContext, lake: LakeStorage):
        day = date.fromisoformat(context.partition_key)
        result = parsing.extract_table_day(spec, day)

        objects_prefix = _objects_prefix_for_day(spec, day)
        if objects_prefix is not None:
            # Cleared before anything is (re)written -- see the module docstring on
            # why stale objects from a shrunk re-run must not survive.
            lake.delete_prefix(objects_prefix)

        frame_objects_written = 0
        for row in result.rows:
            binary_data = row.pop("_binary_data", None)
            if binary_data is not None:
                lake.write_bytes(row["object_key"], binary_data)

            frames = row.pop("_frames", None)
            if frames:
                for key, frame in zip(row["frame_keys"], frames, strict=True):
                    lake.write_bytes(key, frame.data, content_type=frame.content_type)
                    frame_objects_written += 1

        table_prefix = f"{prefix}/dt={day.isoformat()}"
        lake.delete_prefix(table_prefix)
        written_bytes = lake.write_table(
            f"{table_prefix}/data.parquet", pa.Table.from_pylist(result.rows, schema=schema)
        )
        landed_rows = lake.count_rows(f"{table_prefix}/data.parquet") or 0

        context.log.info(
            "%s dt=%s: %d rows extracted, %d landed, %d frame objects, %d frame "
            "decode errors",
            spec.table_name, day, result.count, landed_rows, frame_objects_written,
            result.frame_decode_errors,
        )

        metadata = {
            "rows_extracted": result.count,
            "rows_landed": landed_rows,
            "bytes_written": written_bytes,
            "destination": MetadataValue.path(f"{lake.describe()}/{table_prefix}/data.parquet"),
        }
        if spec.decode_metadata_images:
            metadata["frame_decode_errors"] = result.frame_decode_errors
            metadata["frame_objects_written"] = frame_objects_written

        yield Output({"rows": result.count}, metadata=metadata)

        yield AssetCheckResult(
            check_name=CHECK_ROW_COUNT,
            passed=row_count_check_passed(result.count, landed_rows),
            severity=AssetCheckSeverity.ERROR,
            metadata={"rows_extracted": result.count, "rows_landed": landed_rows},
        )

        if spec.decode_metadata_images:
            expected_frames = sum(row.get("frame_count", 0) for row in result.rows)
            objects_present = lake.count_prefix(objects_prefix) if objects_prefix else 0
            yield AssetCheckResult(
                check_name=CHECK_FRAME_COUNT,
                passed=frame_count_check_passed(expected_frames, objects_present),
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "frames_expected": expected_frames,
                    "frame_objects_present": objects_present,
                    "frame_decode_errors": result.frame_decode_errors,
                },
            )

    return _table_asset


ai_platform_assets = [_make_table_asset(spec) for spec in REGISTRY]

# name -> the specific asset function, for anything that needs to point at exactly
# one table's asset rather than the whole group -- warehouse.py's _SOURCES does.
ai_platform_asset_by_table: dict[str, object] = {
    spec.table_name: asset_fn for spec, asset_fn in zip(REGISTRY, ai_platform_assets, strict=True)
}

snapshot_asset_keys = [
    f"{spec.table_name}_raw" for spec in REGISTRY if spec.mode == MODE_SNAPSHOT
]
window_asset_keys = [
    f"{spec.table_name}_raw" for spec in REGISTRY if spec.mode == MODE_WINDOW
]


# --------------------------------------------------------------------------------
# Registry coverage: does the registry still match the live database?
# --------------------------------------------------------------------------------

CHECK_REGISTRY_COVERAGE = "registry_matches_live_schema"


def live_base_tables(rows: list[tuple] | None = None) -> list[str]:
    """Every base table `ai_platform`'s `public` schema holds, `information_schema`
    itself. `rows` is injectable for tests, the same contract as every extraction
    function in this source."""
    if rows is not None:
        return [r[0] for r in rows]

    import psycopg

    query = (
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %(schema)s AND table_type = 'BASE TABLE'"
    )
    with psycopg.connect(**parsing.connection_settings()) as conn:
        conn.execute("SET TIME ZONE 'UTC';")
        with conn.cursor() as cur:
            cur.execute(query, {"schema": "public"})
            return [row[0] for row in cur.fetchall()]


@asset(
    group_name="ai_platform",
    compute_kind="python",
    description=(
        "Compares the live ai_platform schema against ai_platform_registry.REGISTRY. "
        "A table the platform adds later has no TableSpec and would otherwise never "
        "arrive in the warehouse and never be noticed -- this is what makes that fail "
        "loudly instead."
    ),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_REGISTRY_COVERAGE,
            asset="ai_platform_registry_coverage",
            description=(
                "Fails when a live base table (other than the two deliberately "
                "skipped ones) has no TableSpec in the registry."
            ),
        )
    ],
)
def ai_platform_registry_coverage(context: AssetExecutionContext) -> Output[dict]:
    live = live_base_tables()
    missing, stale = registry_coverage(live)

    if missing:
        context.log.error("tables in ai_platform with no registry entry: %s", missing)
    if stale:
        # Not fatal -- a table can be dropped upstream and the registry simply keeps
        # landing zero rows for it forever, which is safe -- but worth a log line so
        # somebody notices the registry could be trimmed.
        context.log.warning("registry entries with no live table behind them: %s", stale)

    yield Output(
        {"missing": missing, "stale": stale},
        metadata={
            "live_table_count": len(live),
            "registry_table_count": len(REGISTRY),
            "missing_tables": MetadataValue.text(", ".join(missing) or "none"),
            "stale_tables": MetadataValue.text(", ".join(stale) or "none"),
        },
    )
    yield AssetCheckResult(
        check_name=CHECK_REGISTRY_COVERAGE,
        passed=not missing,
        severity=AssetCheckSeverity.ERROR,
        metadata={"missing_tables": missing, "stale_tables": stale},
        description=(
            "Add each missing table to ai_platform_registry.py as a TableSpec (or, "
            "if it should never be ingested, to SKIPPED_TABLES with the reason)."
        ),
    )
