"""primary's journal and auth.log, one UTC day per partition. See parsing/host_logs.py.

The journal is lake-only: [count withheld] entries a day would grow the warehouse by tens of
millions of rows a year, rebuilt in full every night, for data nobody models yet. It is
queryable through the SQL API like every other lake dataset.

Both sources forget: journald past its [volume withheld] cap, logrotate after four weeks. So a day
is never replaced by a read that holds fewer rows than the lake already has -- a re-run
of an old partition keeps what was landed while the host still had it.
"""

import os
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
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
from homelab_datalake.parsing import host_logs
from homelab_datalake.resources import LakeStorage

JOURNAL_PREFIX = layout.prefix("host_journal")
AUTH_PREFIX = layout.prefix("host_auth_log")

# The oldest auth.log line on primary when this was first deployed.
HOST_LOGS_START = "2026-08-16"
host_log_partitions = DailyPartitionsDefinition(start_date=HOST_LOGS_START, timezone="UTC")

CHECK_NOT_SHRUNK = "partition_did_not_shrink"


def _host_log_dir() -> str:
    return os.environ.get("HOST_LOG_DIR", "/host/log")


def _write_if_not_smaller(lake: LakeStorage, key: str, rows_new: int, write) -> tuple[bool, int | None]:
    """Write unless the lake already holds more rows for this partition."""
    rows_held = lakeio.row_count(lake, key) if lakeio.exists(lake, key) else None
    if rows_held is not None and rows_new < rows_held:
        return False, rows_held
    write()
    return True, rows_held


@asset(
    partitions_def=host_log_partitions,
    group_name="host",
    kinds={"python"},
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    check_specs=[AssetCheckSpec(
        name=CHECK_NOT_SHRUNK, asset="host_logs_raw",
        description=(
            "Neither partition was replaced by a read holding fewer rows than the lake had. "
            "When this fails the lake kept its copy: the host has already forgotten part of the day."
        ),
    )],
    description=(
        "One UTC day of primary's systemd journal (every entry, every field) and of "
        "auth.log (sign-ins, sudo, cron sessions)."
    ),
)
def host_logs_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    day = date.fromisoformat(context.partition_key)
    log_dir = _host_log_dir()
    journal_key = f"{JOURNAL_PREFIX}/dt={day.isoformat()}/data.parquet"
    auth_key = f"{AUTH_PREFIX}/dt={day.isoformat()}/data.parquet"

    with tempfile.TemporaryDirectory() as scratch:
        local = Path(scratch) / "journal.parquet"
        writer = None
        journal_rows = 0
        try:
            for table in host_logs.read_journal_day(str(Path(log_dir) / "journal"), day):
                if writer is None:
                    writer = pq.ParquetWriter(local, host_logs.JOURNAL_SCHEMA, compression="zstd")
                writer.write_table(table)
                journal_rows += table.num_rows
        finally:
            if writer is not None:
                writer.close()
        journal_written, journal_held = (False, None)
        if journal_rows:
            journal_written, journal_held = _write_if_not_smaller(
                lake, journal_key, journal_rows, lambda: lakeio.put_file(lake, journal_key, local))

    auth = host_logs.read_auth_logs(log_dir)
    auth_rows = auth.rows_by_day.get(day, [])
    auth_written, auth_held = (False, None)
    if auth_rows:
        auth_written, auth_held = _write_if_not_smaller(
            lake, auth_key, len(auth_rows),
            lambda: lake.write_table(auth_key, pa.Table.from_pylist(auth_rows, schema=host_logs.AUTH_SCHEMA)))

    shrunk = [name for name, written, held, new in (
        ("journal", journal_written, journal_held, journal_rows),
        ("auth_log", auth_written, auth_held, len(auth_rows)),
    ) if not written and held is not None and new < held]
    context.log.info("%s journal=%d (held %s) auth=%d (held %s) unparsed_auth_lines=%d",
                     day, journal_rows, journal_held, len(auth_rows), auth_held, auth.unparsed)
    return MaterializeResult(
        metadata={
            "journal_entries": journal_rows,
            "auth_lines": len(auth_rows),
            "auth_lines_unparsed_all_files": auth.unparsed,
            "journal_written": journal_written,
            "auth_written": auth_written,
        },
        check_results=[AssetCheckResult(
            check_name=CHECK_NOT_SHRUNK, passed=not shrunk,
            metadata={"kept_lake_copy_for": ", ".join(shrunk) or "none"},
        )],
    )


host_logs_job = define_asset_job(
    name="host_logs", selection=[host_logs_raw],
    description="Land one day of primary's journal and auth.log.",
)


@schedule(job=host_logs_job, cron_schedule="5 3 * * *",
          default_status=DefaultScheduleStatus.STOPPED,
          description="Yesterday's journal and auth.log at 03:05 UTC.")
def host_logs_schedule(context: ScheduleEvaluationContext):
    day = (context.scheduled_execution_time.date() - timedelta(days=1)).isoformat()
    yield RunRequest(run_key=f"host-logs-{day}", partition_key=day)


ASSETS = [host_logs_raw]
JOBS = [host_logs_job]
SCHEDULES = [host_logs_schedule]
