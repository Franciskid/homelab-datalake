"""The status page's own history: Secondary's and Tertiary's resources, states and incidents.

Prometheus scrapes the status page since 2026-09-15 (job `status-page`), which covers
from then on. Everything before is only in the two JSON files on its volume, and the
application prunes both to 90 days -- `metrics.json` samples began on 2026-08-08, so
the oldest start disappearing in November. This lands them first and keeps them.

Read from a read-only mount of the `status-page-data` volume. `incidents.json` also
holds browser push subscriptions, whose endpoints are bearer capabilities; that key is
never read.

Samples and observations are merged into daily partitions by their natural key, so a
day the application has pruned keeps what the lake already holds. Incidents,
transitions and the service list change in place, so they are ledgers.
"""

import json
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
from dagster import (
    AssetCheckResult,
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
from homelab_datalake.parsing import ledger
from homelab_datalake.resources import LakeStorage

SAMPLES_PREFIX = layout.prefix("status_page_metric_samples")
OBSERVATIONS_PREFIX = layout.prefix("status_page_observations")
LEDGERS = {
    "status_page_incidents": ("id",),
    "status_page_transitions": ("id",),
    "status_page_services": ("server_id",),
}

_DISK = pa.struct([
    ("mount_point", pa.string()), ("used_bytes", pa.int64()),
    ("total_bytes", pa.int64()), ("usage_percent", pa.float64()),
])

SAMPLE_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("recorded_at", pa.timestamp("ms", tz="UTC")),
    ("server_id", pa.string()),
    ("server_name", pa.string()),
    ("cpu_percent", pa.float64()),
    ("ram_used_bytes", pa.int64()),
    ("ram_total_bytes", pa.int64()),
    ("ram_usage_percent", pa.float64()),
    ("disks", pa.list_(_DISK)),
    ("sample_json", pa.string()),
])

OBSERVATION_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("observed_at", pa.timestamp("ms", tz="UTC")),
    ("server_id", pa.string()),
    ("sentinel_status", pa.string()),
    ("source", pa.string()),
])

CHECK_NOT_SHRUNK = "no_partition_shrank"


def _instant(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _int(value) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _float(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def sample_rows(metrics: dict) -> list[dict]:
    rows = []
    for sample in metrics.get("samples") or []:
        recorded = _instant(sample.get("recordedAt"))
        if recorded is None or not sample.get("serverId"):
            continue
        ram = sample.get("ram") or {}
        rows.append({
            "dt": recorded.date(),
            "recorded_at": recorded,
            "server_id": sample["serverId"],
            "server_name": sample.get("serverName"),
            "cpu_percent": _float(sample.get("cpuPercent")),
            "ram_used_bytes": _int(ram.get("usedBytes")),
            "ram_total_bytes": _int(ram.get("totalBytes")),
            "ram_usage_percent": _float(ram.get("usagePercent")),
            "disks": [
                {"mount_point": d.get("mountPoint"), "used_bytes": _int(d.get("usedBytes")),
                 "total_bytes": _int(d.get("totalBytes")), "usage_percent": _float(d.get("usagePercent"))}
                for d in sample.get("disks") or []
            ],
            "sample_json": json.dumps(sample, sort_keys=True),
        })
    return rows


def observation_rows(state: dict) -> list[dict]:
    rows = []
    for observation in state.get("observations") or []:
        observed = _instant(observation.get("observedAt"))
        if observed is None or not observation.get("serverId"):
            continue
        rows.append({
            "dt": observed.date(), "observed_at": observed,
            "server_id": observation["serverId"],
            "sentinel_status": observation.get("sentinelStatus"),
            "source": observation.get("source"),
        })
    return rows


def ledger_rows(state: dict) -> dict[str, list[dict]]:
    """Incidents, transitions and services. `pushSubscriptions` is never touched."""
    services = [
        {"server_id": server_id, **(service or {})}
        for server_id, service in (state.get("services") or {}).items()
    ]
    return {
        "status_page_incidents": list(state.get("incidents") or []),
        "status_page_transitions": list(state.get("transitions") or []),
        "status_page_services": services,
    }


def merge_partitions(lake: LakeStorage, prefix: str, rows: list[dict], schema: pa.Schema,
                     key: tuple[str, ...]) -> dict:
    """Union each day's rows into its partition. Returns what was written."""
    by_day: dict[date, list[dict]] = defaultdict(list)
    for row in rows:
        by_day[row["dt"]].append(row)
    report = {"partitions_written": 0, "rows_added": 0, "shrunk": []}
    for day, day_rows in sorted(by_day.items()):
        object_key = f"{prefix}/dt={day.isoformat()}/data.parquet"
        existing = lakeio.read_strict(lake, object_key)
        merged: dict[tuple, dict] = {}
        if existing is not None:
            for row in existing.to_pylist():
                merged[tuple(row[k] for k in key)] = row
        before = len(merged)
        for row in day_rows:
            merged.setdefault(tuple(row[k] for k in key), row)
        if len(merged) == before and existing is not None:
            continue
        if len(merged) < before:
            report["shrunk"].append(day.isoformat())
            continue
        ordered = sorted(merged.values(), key=lambda r: tuple(str(r[k]) for k in key))
        lake.write_table(object_key, pa.Table.from_pylist(ordered, schema=schema))
        report["partitions_written"] += 1
        report["rows_added"] += len(merged) - before
    return report


def capture(lake: LakeStorage, data_dir: str, captured_at: datetime) -> dict:
    directory = Path(data_dir)
    metrics = json.loads((directory / "metrics.json").read_text())
    state = json.loads((directory / "incidents.json").read_text())
    report = {
        "metric_samples": merge_partitions(
            lake, SAMPLES_PREFIX, sample_rows(metrics), SAMPLE_SCHEMA, ("server_id", "recorded_at")),
        "observations": merge_partitions(
            lake, OBSERVATIONS_PREFIX, observation_rows(state), OBSERVATION_SCHEMA,
            ("server_id", "observed_at", "source")),
    }
    for table_name, rows in ledger_rows(state).items():
        object_key = f"{layout.prefix(table_name)}/data.parquet"
        table, stats = ledger.merge(
            lakeio.read_strict(lake, object_key), rows, LEDGERS[table_name], captured_at)
        lake.write_table(object_key, table)
        report[table_name] = {"versions": stats.versions_after, "new_versions": stats.new_versions}
    return report


@asset(
    group_name="status_page",
    kinds={"python"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    check_specs=[AssetCheckSpec(
        name=CHECK_NOT_SHRUNK, asset="status_page_raw",
        description=(
            "No day of samples or observations holds fewer rows than the lake already had. "
            "The status page prunes after 90 days; the lake must not follow it."
        ),
    )],
    description=(
        "Secondary's and Tertiary's cpu, memory and disk samples, their UP/DOWN observations, "
        "incidents and transitions, from the status page's volume. Merged, never replaced."
    ),
)
def status_page_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    report = capture(lake, os.environ.get("STATUS_PAGE_DATA_DIR", "/sources/status-page"), datetime.now(UTC))
    context.log.info("%s", report)
    shrunk = report["metric_samples"]["shrunk"] + report["observations"]["shrunk"]
    return MaterializeResult(
        metadata={
            "samples_added": report["metric_samples"]["rows_added"],
            "observations_added": report["observations"]["rows_added"],
            "detail": MetadataValue.json(report),
        },
        check_results=[AssetCheckResult(
            check_name=CHECK_NOT_SHRUNK, passed=not shrunk,
            metadata={"days": ", ".join(shrunk) or "none"},
        )],
    )


status_page_job = define_asset_job(
    name="status_page", selection=[status_page_raw],
    description="Merge the status page's samples, observations and incidents into the lake.",
)


@schedule(job=status_page_job, cron_schedule="55 */6 * * *",
          default_status=DefaultScheduleStatus.STOPPED,
          description="Every six hours; the files hold 90 days, so a missed run loses nothing.")
def status_page_schedule(_context):
    return {}


ASSETS = [status_page_raw]
JOBS = [status_page_job]
SCHEDULES = [status_page_schedule]
