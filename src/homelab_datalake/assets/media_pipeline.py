"""Radarr, Sonarr and Jellyseerr: requests and download history, as ledgers.

Read from each application's own SQLite database through a read-only mount of its
config directory (see parsing/sqlite_source.py), rather than through their REST APIs.
Two reasons, both measured: the database needs no API key in the datalake's
environment, and it holds what the APIs page through or never expose -- the download
client's side of every download, blocklists, collections, and history for titles the
API only returns while the title still exists.

Each table is one object, `<prefix>/data.parquet`, merged on every run. Radarr and
Sonarr delete a title's history when the title is removed; the ledger keeps it.
"""

import os
from datetime import UTC, datetime

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DefaultScheduleStatus,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
    define_asset_job,
    schedule,
)

from homelab_datalake import lakeio, layout
from homelab_datalake.parsing import ledger, sqlite_source
from homelab_datalake.parsing.media_pipeline import APPS, DB_PATH_ENV, TableSpec
from homelab_datalake.resources import LakeStorage

CHECK_TABLES_PRESENT = "every_table_was_read"


def ledger_key(spec: TableSpec) -> str:
    return f"{layout.prefix(spec.table_name)}/data.parquet"


def db_path(app: str) -> str:
    variable, default = DB_PATH_ENV[app]
    return os.environ.get(variable, default)


def capture(lake: LakeStorage, app: str, path: str, captured_at: datetime) -> dict:
    """Merge one consistent read of every registered table into its ledger."""
    report = {"tables": {}, "missing": []}
    with sqlite_source.snapshot(path) as connection:
        for spec in APPS[app]:
            try:
                rows, key, _columns = sqlite_source.read_table(connection, spec.source_table, spec.exclude)
            except KeyError:
                report["missing"].append(spec.source_table)
                continue
            if spec.redact:
                rows = [{k: ledger.redact_secrets(ledger.as_text(v)) for k, v in row.items()} for row in rows]
            table, stats = ledger.merge(
                lakeio.read_strict(lake, ledger_key(spec)), rows, key, captured_at, spec.volatile,
            )
            written = lake.write_table(ledger_key(spec), table)
            report["tables"][spec.table_name] = {
                "rows_in_source": len(rows),
                "versions": stats.versions_after,
                "new_versions": stats.new_versions,
                "gone_from_source": stats.keys_gone_from_source,
                "bytes": written,
            }
    return report


def _make_asset(app: str):
    specs = APPS[app]

    @asset(
        name=f"{app}_raw",
        group_name="media_pipeline",
        kinds={"python", "sqlite"},
        retry_policy=RetryPolicy(max_retries=3, delay=60),
        check_specs=[AssetCheckSpec(
            name=CHECK_TABLES_PRESENT, asset=f"{app}_raw",
            description=(
                f"Every table registered for {app} exists in its database. An upgrade that "
                "renames a table would otherwise stop that history landing without a sound."
            ),
        )],
        description=(
            f"{app.capitalize()}'s " + ", ".join(s.name.replace("_", " ") for s in specs)
            + ", merged into one ledger per table: every distinct version of every row."
        ),
    )
    def _asset(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
        report = capture(lake, app, db_path(app), datetime.now(UTC))
        for name, stats in report["tables"].items():
            context.log.info("%s %s", name, stats)
        return MaterializeResult(
            metadata={
                "tables": len(report["tables"]),
                "new_versions": sum(t["new_versions"] for t in report["tables"].values()),
                "versions": sum(t["versions"] for t in report["tables"].values()),
                "detail": MetadataValue.json(report["tables"]),
            },
            check_results=[AssetCheckResult(
                check_name=CHECK_TABLES_PRESENT, passed=not report["missing"],
                severity=AssetCheckSeverity.WARN,
                metadata={"missing": ", ".join(report["missing"]) or "none"},
            )],
        )

    return _asset


radarr_raw = _make_asset("radarr")
sonarr_raw = _make_asset("sonarr")
jellyseerr_raw = _make_asset("jellyseerr")

media_pipeline_job = define_asset_job(
    name="media_pipeline",
    selection=[radarr_raw, sonarr_raw, jellyseerr_raw],
    description="Merge Radarr, Sonarr and Jellyseerr's requests, downloads and lookups into their ledgers.",
)


@schedule(
    job=media_pipeline_job, cron_schedule="50 */6 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Every six hours. A request can go from pending to available within a day, and "
        "a ledger only sees the states it is there to read."
    ),
)
def media_pipeline_schedule(_context):
    return {}


ASSETS = [radarr_raw, sonarr_raw, jellyseerr_raw]
JOBS = [media_pipeline_job]
SCHEDULES = [media_pipeline_schedule]
