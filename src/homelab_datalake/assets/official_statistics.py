"""Official statistics for politico.fraserv.fr: Eurostat and the SSMSI, landed and exported.

Every query in parsing/official_statistics.py is read once a day. The values land in one
ledger, `official_stat_observations`, keyed by publisher, dataset, series, place and period:
a revision by the publisher becomes a new version of the row, and the previous figure stays
queryable. Then the day's figures are written as one JSON file for politico.fraserv.fr's
renderer, which turns them into charts and pages.

The export directory is a bind mount shared with the politico stack. Without it (a test, a
laptop) the ledger still lands and the export is reported as skipped.
"""


import json
import os
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DefaultScheduleStatus,
    MaterializeResult,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

from homelab_datalake import lakeio, layout
from homelab_datalake.parsing import ledger
from homelab_datalake.parsing import official_statistics as stats
from homelab_datalake.resources import LakeStorage

LEDGER_KEY = f"{layout.prefix('official_stat_observations')}/data.parquet"
LEDGER_SCHEMA = pa.schema([(column, pa.string()) for column in stats.LEDGER_COLUMNS] + list(ledger.META_SCHEMA))
DEFAULT_EXPORT_DIR = "/exports/politico"
EXPORT_NAME = "official-statistics.json"
REQUEST_TIMEOUT_SECONDS = 180
ATTEMPTS = 3
USER_AGENT = "fraserv-datalake/1.0 (official statistics)"

CHECK_ALL_ANSWERED = "every_publisher_answered"
CHECK_EXPORTED = "export_written"


def _get(url: str) -> bytes:
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https source: {url!r}")
    last: Exception | None = None
    for attempt in range(ATTEMPTS):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})  # noqa: S310
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
                return response.read()
        except Exception as error:  # noqa: BLE001 -- retried, then re-raised with its cause
            last = error
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url} failed {ATTEMPTS} times") from last


def fetch_eurostat(query: stats.EurostatQuery) -> dict:
    payload = json.loads(_get(query.url()))
    if "id" not in payload:
        # The API answers large requests asynchronously with a message instead of data.
        raise RuntimeError(f"{query.code}: Eurostat did not return a dataset: {str(payload)[:300]}")
    return payload


def fetch_ssmsi() -> tuple[list[dict], dict]:
    resource = stats.ssmsi_resource(json.loads(_get(stats.SSMSI_DATASET_API)))
    text = _get(resource["url"]).decode("utf-8")
    return stats.ssmsi_rows(text), resource


def read_previous_export(directory: Path) -> dict | None:
    path = directory / EXPORT_NAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_export(directory: Path, export: dict) -> Path:
    """Write the file whole, then rename it over the old one: the renderer never reads half."""
    path = directory / EXPORT_NAME
    tmp = directory / f".{EXPORT_NAME}.tmp"
    tmp.write_text(json.dumps(export, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
    return path


def collect(log) -> tuple[list[dict], dict[str, dict], list[str]]:
    """Read every publisher. Returns ledger rows, export blocks, and what failed."""
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []
    for query in stats.EUROSTAT_QUERIES:
        try:
            payload = fetch_eurostat(query)
        except Exception as error:  # noqa: BLE001 -- one dataset down must not stop the others
            log.warning("eurostat %s failed: %s", query.code, error)
            failed.append(f"eurostat:{query.code}")
            continue
        rows.extend(stats.eurostat_ledger_rows(query, payload))
        blocks[stats.dataset_key("eurostat", query.code)] = stats.eurostat_block(query, payload, fetched_at)
    try:
        ssmsi, resource = fetch_ssmsi()
        rows.extend(stats.ssmsi_ledger_rows(ssmsi, resource))
        blocks[stats.dataset_key("ssmsi", stats.SSMSI_CODE)] = stats.ssmsi_block(ssmsi, resource, fetched_at)
    except Exception as error:  # noqa: BLE001
        log.warning("ssmsi failed: %s", error)
        failed.append(f"ssmsi:{stats.SSMSI_CODE}")
    return rows, blocks, failed


@asset(
    group_name="official_statistics",
    kinds={"python", "api"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    check_specs=[
        AssetCheckSpec(name=CHECK_ALL_ANSWERED, asset="official_statistics_raw",
                       description="Warns when a Eurostat query or the SSMSI file could not be read today."),
        AssetCheckSpec(name=CHECK_EXPORTED, asset="official_statistics_raw",
                       description="Warns when the export for politico.fraserv.fr was not written."),
    ],
    description=(
        "Eurostat series and the SSMSI's recorded crime, every version of every figure, plus "
        "the day's export for politico.fraserv.fr's Données section."
    ),
)
def official_statistics_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    captured_at = datetime.now(UTC)
    rows, blocks, failed = collect(context.log)
    if not rows:
        raise RuntimeError("no publisher answered; nothing to land")
    merged, merge_stats = ledger.merge(lakeio.read_strict(lake, LEDGER_KEY), rows, stats.KEY_COLUMNS,
                                       captured_at, stats.VOLATILE_COLUMNS)
    lake.write_table(LEDGER_KEY, merged)

    directory = Path(os.environ.get("OFFICIAL_STATS_EXPORT_DIR", DEFAULT_EXPORT_DIR))
    exported = None
    if directory.is_dir():
        export = stats.build_export(blocks, read_previous_export(directory),
                                    captured_at.isoformat(timespec="seconds"))
        exported = str(write_export(directory, export))
    else:
        context.log.warning("export directory %s is not mounted; export skipped", directory)

    context.log.info("official statistics: %d values read, %d new versions, %d datasets, failed: %s",
                     len(rows), merge_stats.new_versions, len(blocks), failed or "none")
    return MaterializeResult(
        metadata={"values_read": len(rows), "new_versions": merge_stats.new_versions,
                  "current_keys": merge_stats.current_keys, "datasets": len(blocks),
                  "failed": ", ".join(failed) or "none", "export": exported or "skipped"},
        check_results=[
            AssetCheckResult(check_name=CHECK_ALL_ANSWERED, passed=not failed,
                             severity=AssetCheckSeverity.WARN, metadata={"failed": ", ".join(failed) or "none"}),
            AssetCheckResult(check_name=CHECK_EXPORTED, passed=exported is not None,
                             severity=AssetCheckSeverity.WARN, metadata={"path": exported or str(directory)}),
        ],
    )


official_statistics_job = define_asset_job(
    name="official_statistics", selection=[official_statistics_raw],
    description="Read Eurostat and the SSMSI, land every figure, export them for politico.fraserv.fr.",
)


@schedule(job=official_statistics_job, cron_schedule="40 5 * * *",
          default_status=DefaultScheduleStatus.RUNNING,
          description=(
              "Daily at 05:40 UTC. Eurostat releases at 11:00 and 23:00 Luxembourg time and the "
              "SSMSI a few times a year, so one morning read is never more than a few hours late."
          ))
def official_statistics_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"official-statistics-{context.scheduled_execution_time:%Y%m%d}")


ASSETS = [official_statistics_raw]
JOBS = [official_statistics_job]
SCHEDULES = [official_statistics_schedule]
