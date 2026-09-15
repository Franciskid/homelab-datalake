"""Prometheus into the lake, as a stream.

Dagster schedules and tracks work; it does not process streams. So the stream is a
sensor that writes a small batch every minute, and everything that makes the result
trustworthy is ordinary partitioned assets behind it. See
docs/METRICS_STREAMING_PLAN.md for the investigation this is built on.

**Prometheus is the buffer.** It keeps every raw sample for 30 days (raised from 15 on
2026-09-15 for exactly this reason). The sensor only ever reads from a cursor up to
`now - 60 s`, so a stopped daemon, a crashed tick or a week-long outage costs latency,
never data: whatever the stream missed is still in Prometheus, and the hourly
compaction re-reads it.

**Three kinds of file, one partition column.** Under `dt=YYYY-MM-DD/`:

    part-<start_ms>-<end_ms>.parquet   written by the sensor every tick
    hour-HH.parquet                    written by prometheus_samples_raw, hourly at :25
    data.parquet                       written by prometheus_samples_compacted, at 00:20

A query over today opens at most [count withheld] objects; over any earlier day, one. Mixing an
`hour=` directory level into only some files would break DuckDB's partition reading
when a view unions them, which is why the hour is in the file name instead.

**Completeness is measured against the source.** Compacting an hour asks Prometheus
how many samples it holds for each job in that hour (`count_over_time`) and compares.
A job that is short -- the sensor was down, a tick failed, the stream never saw it --
is re-read in full from Prometheus before the hour file is written.

**Not in the warehouse.** [count withheld] samples a day would multiply the DuckDB file within a
week, and the warehouse is rebuilt from the lake in full every night. The SQL API
already serves every lake dataset as a view over its Parquet, so the samples are
queryable from the Data workspace without being copied.
"""

import json
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    BackfillPolicy,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    HourlyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset,
    define_asset_job,
    schedule,
    sensor,
)

from homelab_datalake import lakeio, layout
from homelab_datalake.parsing import prometheus as prom
from homelab_datalake.resources import LakeStorage

SAMPLES_PREFIX = layout.prefix("prometheus_samples")
SERIES_PREFIX = layout.prefix("prometheus_series")
METADATA_PREFIX = layout.prefix("prometheus_metadata")

# The oldest hour Prometheus still held when the lake first read it (2026-08-30 12:59:57).
STREAM_START = "2026-08-30-12:00"
STREAM_START_MS = prom.ms_of(datetime(2026, 8, 30, 12, tzinfo=UTC))

# A sample is timestamped when its scrape starts and stored when the scrape ends, up
# to the [duration withheld] scrape timeout later. Reading closer to now than this could move the
# cursor past a sample that has not been appended yet.
LAG_MS = 60_000
# The most one tick reads. Keeps a tick well inside Dagster's [duration withheld] sensor timeout;
# anything older is left to the hourly compaction, which re-reads from Prometheus.
MAX_TICK_MS = 15 * prom.MINUTE_MS
# How far behind the stream may fall before the hourly run fails and opens an incident.
STALE_AFTER_MS = 10 * prom.MINUTE_MS
# Missed hours handed to the run queue per tick. The queue holds ten runs for the
# whole Dagster instance, the AI platform's automations included.
REPAIRS_PER_TICK = 2

hourly_partitions = HourlyPartitionsDefinition(start_date=STREAM_START, timezone="UTC")
daily_partitions = DailyPartitionsDefinition(start_date=STREAM_START[:10], timezone="UTC")

CHECK_MATCHES_PROMETHEUS = "hour_matches_prometheus"
CHECK_NO_DUPLICATES = "no_duplicate_samples"
CHECK_STREAM_FRESH = "stream_is_fresh"
CHECK_JOBS_DOCUMENTED = "every_job_is_documented"
CHECK_DAY_COMPLETE = "day_holds_every_hour"


def client_from_env() -> prom.PrometheusClient:
    url = os.environ.get("PROMETHEUS_URL", "")
    if not url:
        raise ValueError(
            "PROMETHEUS_URL is not set. On primary it is http://198.51.100.26:9090 -- "
            "the address datalake-daemon reaches Prometheus's published port on."
        )
    return prom.PrometheusClient(url)


def _now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------------
# object names
# --------------------------------------------------------------------------------

_PART = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/part-(\d+)-(\d+)\.parquet$")
_HOUR = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/hour-(\d{2})\.parquet$")
_DAY = re.compile(r"/dt=(\d{4}-\d{2}-\d{2})/data\.parquet$")


def day_prefix(prefix: str, day: date) -> str:
    return f"{prefix}/dt={day.isoformat()}"


def part_key(prefix: str, start_ms: int, end_ms: int) -> str:
    return f"{day_prefix(prefix, prom.day_of(start_ms))}/part-{start_ms}-{end_ms}.parquet"


def hour_key(prefix: str, hour_ms: int) -> str:
    moment = datetime.fromtimestamp(hour_ms / 1000, UTC)
    return f"{day_prefix(prefix, moment.date())}/hour-{moment.hour:02d}.parquet"


def day_key(prefix: str, day: date) -> str:
    return f"{day_prefix(prefix, day)}/data.parquet"


def _day_start_ms(day: date) -> int:
    return prom.ms_of(datetime(day.year, day.month, day.day, tzinfo=UTC))


@dataclass
class DayListing:
    parts: dict[tuple[int, int], str] = field(default_factory=dict)
    hours: dict[int, str] = field(default_factory=dict)
    day: str | None = None

    def parts_in_hour(self, hour_ms: int) -> list[str]:
        return [key for (start, _end), key in sorted(self.parts.items())
                if hour_ms <= start < hour_ms + prom.HOUR_MS]

    def covered_until(self) -> int | None:
        ends = [end for (_start, end) in self.parts]
        ends += [hour + prom.HOUR_MS for hour in self.hours]
        return max(ends) if ends else None


def list_day(lake: LakeStorage, prefix: str, day: date) -> DayListing:
    listing = DayListing()
    for key in lakeio.list_keys(lake, day_prefix(prefix, day)):
        if match := _PART.search(key):
            listing.parts[(int(match.group(2)), int(match.group(3)))] = key
        elif match := _HOUR.search(key):
            listing.hours[_day_start_ms(date.fromisoformat(match.group(1)))
                          + int(match.group(2)) * prom.HOUR_MS] = key
        elif _DAY.search(key):
            listing.day = key
    return listing


def stream_position(lake: LakeStorage, now_ms: int) -> int | None:
    """How far the lake already holds samples, read from object names alone.

    Used when the sensor has no cursor -- first start, or a reset. Today and yesterday
    are enough: anything older is past what one tick would read anyway.
    """
    today = prom.day_of(now_ms)
    positions = []
    for day in (today - timedelta(days=1), today):
        listing = list_day(lake, SAMPLES_PREFIX, day)
        if listing.day:
            positions.append(_day_start_ms(day) + 24 * prom.HOUR_MS)
        if (covered := listing.covered_until()) is not None:
            positions.append(covered)
    return max(positions) if positions else None


# --------------------------------------------------------------------------------
# the stream
# --------------------------------------------------------------------------------

# Series already written for an hour by this process. The sensor runs in a long-lived
# code server, so this usually saves writing [count withheld] label sets every minute; when the
# process restarts it is empty, a few label sets are written twice, and compaction
# removes the duplicates. Correctness never depends on it.
_EMITTED_SERIES: dict[int, set[int]] = {}


def write_parts(lake: LakeStorage, batch: prom.Batch, start_ms: int, end_ms: int,
                emitted: dict[int, set[int]]) -> tuple[int, int]:
    """One part per hour the batch touches. Returns (objects, series written)."""
    objects = series_written = 0
    for hour, sub in sorted(batch.split_by_hour().items()):
        part_start, part_end = max(start_ms, hour), min(end_ms, hour + prom.HOUR_MS)
        lake.write_table(part_key(SAMPLES_PREFIX, part_start, part_end), sub.samples_table())
        objects += 1
        seen = emitted.setdefault(hour, set())
        new = set(sub.series) - seen
        if new:
            lake.write_table(part_key(SERIES_PREFIX, part_start, part_end), sub.series_table(new))
            seen |= new
            series_written += len(new)
            objects += 1
    for hour in [h for h in emitted if h < prom.hour_floor(end_ms) - prom.HOUR_MS]:
        del emitted[hour]
    return objects, series_written


@dataclass
class Tick:
    start_ms: int
    end_ms: int
    samples: int = 0
    series: int = 0
    objects: int = 0
    repairs: list[int] = field(default_factory=list)
    pending: list[int] = field(default_factory=list)
    message: str = ""

    def cursor(self) -> str:
        return json.dumps({"end_ms": self.end_ms, "pending": self.pending})


def parse_cursor(cursor: str | None) -> tuple[int | None, list[int]]:
    if not cursor:
        return None, []
    try:
        data = json.loads(cursor)
    except ValueError:
        return int(cursor), []
    if isinstance(data, int):
        return data, []
    return data.get("end_ms"), list(data.get("pending") or [])


def stream_tick(lake: LakeStorage, client, cursor: str | None, now_ms: int,
                emitted: dict[int, set[int]] | None = None) -> Tick:
    """Read `(cursor, now - lag]` from Prometheus and write it as parts."""
    emitted = _EMITTED_SERIES if emitted is None else emitted
    end = now_ms - LAG_MS
    end -= end % 1000
    start, pending = parse_cursor(cursor)
    if start is None:
        start = stream_position(lake, now_ms) or prom.hour_floor(end)

    if end - start > MAX_TICK_MS:
        resume = end - MAX_TICK_MS
        # Whole hours before the one `resume` falls in are handed to the hourly job,
        # which re-reads them from Prometheus. The hour `resume` falls in is repaired
        # by its own scheduled compaction.
        hour = prom.hour_floor(start)
        while hour < prom.hour_floor(resume):
            if hour not in pending:
                pending.append(hour)
            hour += prom.HOUR_MS
        start = resume

    repairs, pending = pending[:REPAIRS_PER_TICK], pending[REPAIRS_PER_TICK:]
    tick = Tick(start_ms=start, end_ms=max(start, end), repairs=repairs, pending=pending)
    if end - start < 1000:
        tick.message = "nothing new to read"
        return tick

    batch = prom.Batch()
    for job in client.jobs(start, end):
        batch.extend(client.pull(job, start, end, window_ms=MAX_TICK_MS))
    tick.objects, tick.series = write_parts(lake, batch, start, end, emitted)
    tick.samples = len(batch)
    tick.message = (
        f"wrote {tick.samples:,} samples and {tick.series:,} new series in "
        f"{tick.objects} objects through "
        f"{datetime.fromtimestamp(end / 1000, UTC):%H:%M:%S}Z, lag {(now_ms - end) / 1000:.0f} s"
    )
    return tick


# --------------------------------------------------------------------------------
# compaction
# --------------------------------------------------------------------------------


def _duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC';")
    return con


def dedupe_samples(tables: list[pa.Table]) -> tuple[pa.Table, int]:
    """Union, one row per (series_id, ts), later inputs winning. Returns rows removed."""
    tables = [t.select(prom.SAMPLE_SCHEMA.names).cast(prom.SAMPLE_SCHEMA) for t in tables if t.num_rows]
    if not tables:
        return prom.SAMPLE_SCHEMA.empty_table(), 0
    combined = pa.concat_tables(tables)
    combined = combined.append_column("_ord", pa.array(range(combined.num_rows), pa.int64()))
    con = _duckdb()
    con.register("t", combined)
    out = con.execute(
        "SELECT dt, ts, series_id, metric, job, instance, value FROM ("
        "  SELECT *, row_number() OVER (PARTITION BY series_id, ts ORDER BY _ord DESC) AS rn FROM t"
        ") WHERE rn = 1 ORDER BY job, metric, series_id, ts"
    ).to_arrow_table().cast(prom.SAMPLE_SCHEMA)
    con.close()
    return out, combined.num_rows - out.num_rows


def dedupe_series(tables: list[pa.Table]) -> pa.Table:
    """One row per series id, with the widest first/last seen across the inputs."""
    tables = [t.select(prom.SERIES_SCHEMA.names).cast(prom.SERIES_SCHEMA) for t in tables if t.num_rows]
    if not tables:
        return prom.SERIES_SCHEMA.empty_table()
    con = _duckdb()
    con.register("t", pa.concat_tables(tables))
    out = con.execute(
        "SELECT CAST(min(first_seen) AS DATE) AS dt, series_id, any_value(metric) AS metric,"
        "  any_value(job) AS job, any_value(instance) AS instance, any_value(labels) AS labels,"
        "  min(first_seen) AS first_seen, max(last_seen) AS last_seen"
        " FROM t GROUP BY series_id ORDER BY job, metric, series_id"
    ).to_arrow_table().cast(prom.SERIES_SCHEMA)
    con.close()
    return out


def _is_short(job: str, samples: int, expected: int, scrapes: int, expected_scrapes: int) -> bool:
    return scrapes < expected_scrapes or samples < expected * (1 - prom.tolerance(job))


def _scrapes_by_job(table: pa.Table) -> dict[str, int]:
    if not table.num_rows:
        return {}
    return _counts_by_job(table.filter(pc.equal(table.column("metric"), "up")))


def _counts_by_job(table: pa.Table) -> dict[str, int]:
    if not table.num_rows:
        return {}
    grouped = table.group_by("job").aggregate([("ts", "count")])
    return dict(zip(grouped.column("job").to_pylist(), grouped.column("ts_count").to_pylist(), strict=True))


@dataclass
class HourResult:
    hour_ms: int
    rows: int = 0
    series: int = 0
    bytes_written: int = 0
    parts_merged: int = 0
    duplicates_removed: int = 0
    expected: dict[str, int] = field(default_factory=dict)
    landed: dict[str, int] = field(default_factory=dict)
    expected_scrapes: dict[str, int] = field(default_factory=dict)
    landed_scrapes: dict[str, int] = field(default_factory=dict)
    reread: list[str] = field(default_factory=list)
    native_histogram_series: int = 0
    already_in_day_file: bool = False

    @property
    def short(self) -> dict[str, dict[str, int]]:
        return {
            job: {"samples": self.landed.get(job, 0), "expected": expected,
                  "scrapes": self.landed_scrapes.get(job, 0),
                  "expected_scrapes": self.expected_scrapes.get(job, 0)}
            for job, expected in self.expected.items()
            if _is_short(job, self.landed.get(job, 0), expected,
                         self.landed_scrapes.get(job, 0), self.expected_scrapes.get(job, 0))
        }

    @property
    def label(self) -> str:
        return datetime.fromtimestamp(self.hour_ms / 1000, UTC).strftime("%Y-%m-%d %H:00")


def _read_all(lake: LakeStorage, keys: list[str]) -> list[pa.Table]:
    tables = []
    for key in keys:
        table = lakeio.read_strict(lake, key)
        if table is not None:
            tables.append(table)
    return tables


def _counts_in_day_file(lake: LakeStorage, key: str, hour_ms: int) -> tuple[dict[str, int], dict[str, int]]:
    con = _duckdb()
    lake.configure_duckdb(con)
    path = f"s3://{lake.bucket}/{key}" if lake.uses_s3 else str(Path(lake.local_root) / key)
    rows = con.execute(
        "SELECT job, count(*), count(*) FILTER (WHERE metric = 'up') FROM read_parquet(?)"
        " WHERE ts >= ? AND ts < ? GROUP BY job",
        [path, datetime.fromtimestamp(hour_ms / 1000, UTC),
         datetime.fromtimestamp((hour_ms + prom.HOUR_MS) / 1000, UTC)],
    ).fetchall()
    con.close()
    return {job: n for job, n, _ in rows}, {job: up for job, _, up in rows}


def compact_hour(lake: LakeStorage, client, hour_ms: int, now_ms: int) -> HourResult:
    """Turn one closed hour into `hour-HH.parquet`, complete against Prometheus."""
    end_ms = hour_ms + prom.HOUR_MS
    if end_ms > now_ms - LAG_MS - MAX_TICK_MS:
        raise ValueError(
            f"{datetime.fromtimestamp(hour_ms / 1000, UTC):%Y-%m-%d %H:00} is still open "
            "to the stream; compacting it now would race the sensor's next part"
        )
    result = HourResult(hour_ms=hour_ms)
    day = prom.day_of(hour_ms)
    jobs = client.jobs(hour_ms, end_ms)
    result.expected = {job: client.sample_count(job, hour_ms, end_ms) for job in jobs}
    result.expected_scrapes = {job: client.scrape_count(job, hour_ms, end_ms) for job in jobs}

    samples_listing = list_day(lake, SAMPLES_PREFIX, day)
    if samples_listing.day:
        # The day is already compacted. Report what it holds for this hour, write
        # nothing: rewriting an hour into a finished day would need the whole day in
        # memory, and the daily compaction re-reads any short hour before it merges.
        result.already_in_day_file = True
        result.landed, result.landed_scrapes = _counts_in_day_file(lake, samples_listing.day, hour_ms)
        result.rows = sum(result.landed.values())
        return result

    series_listing = list_day(lake, SERIES_PREFIX, day)
    sample_inputs = samples_listing.parts_in_hour(hour_ms)
    series_inputs = series_listing.parts_in_hour(hour_ms)
    if hour_ms in samples_listing.hours:
        sample_inputs = [samples_listing.hours[hour_ms], *sample_inputs]
    if hour_ms in series_listing.hours:
        series_inputs = [series_listing.hours[hour_ms], *series_inputs]
    result.parts_merged = len(sample_inputs)

    samples, removed = dedupe_samples(_read_all(lake, sample_inputs))
    series_tables = _read_all(lake, series_inputs)
    landed = _counts_by_job(samples)
    landed_scrapes = _scrapes_by_job(samples)

    labelled = set()
    for table in series_tables:
        labelled.update(table.column("series_id").to_pylist())
    unlabelled_jobs = set()
    if samples.num_rows:
        ids = samples.column("series_id").to_pylist()
        jobs = samples.column("job").to_pylist()
        unlabelled_jobs = {job for sid, job in zip(ids, jobs, strict=True) if sid not in labelled}

    reread = sorted(
        {job for job, expected in result.expected.items()
         if _is_short(job, landed.get(job, 0), expected,
                      landed_scrapes.get(job, 0), result.expected_scrapes.get(job, 0))}
        | unlabelled_jobs
    )
    if reread:
        pulled = prom.Batch()
        for job in reread:
            pulled.extend(client.pull(job, hour_ms, end_ms))
        result.native_histogram_series = pulled.native_histogram_series
        # A union, not a replacement: if Prometheus has already dropped part of the
        # hour, what the stream wrote earlier is still the better record.
        samples, removed_again = dedupe_samples([samples, pulled.samples_table()])
        removed += removed_again
        series_tables.append(pulled.series_table())
    result.reread = reread

    series = dedupe_series(series_tables)
    result.rows, result.series = samples.num_rows, series.num_rows
    result.duplicates_removed = removed
    result.landed = _counts_by_job(samples)
    result.landed_scrapes = _scrapes_by_job(samples)

    result.bytes_written = lake.write_table(hour_key(SAMPLES_PREFIX, hour_ms), samples)
    result.bytes_written += lake.write_table(hour_key(SERIES_PREFIX, hour_ms), series)
    # Only after both are written. Deleting first would leave a window in which the
    # hour exists nowhere in the lake.
    lakeio.delete_keys(lake, samples_listing.parts_in_hour(hour_ms))
    lakeio.delete_keys(lake, series_listing.parts_in_hour(hour_ms))
    return result


@dataclass
class DayResult:
    day: date
    rows: int = 0
    series: int = 0
    bytes_written: int = 0
    hours_merged: int = 0
    hours_compacted_here: list[str] = field(default_factory=list)
    rows_in_hours: int = 0
    already_compacted: bool = False


def compact_day(lake: LakeStorage, client, day: date, now_ms: int) -> DayResult:
    """Merge a finished day's hour files into `data.parquet`.

    Any hour without its file is compacted first, from parts or straight from
    Prometheus, so a day whose hourly schedule never ran still ends up whole.
    Streamed row group by row group: a day is [count withheld] samples and is never held in
    memory at once.
    """
    result = DayResult(day=day)
    samples_listing = list_day(lake, SAMPLES_PREFIX, day)
    if samples_listing.day and not samples_listing.hours and not samples_listing.parts:
        result.already_compacted = True
        result.rows = lakeio.row_count(lake, samples_listing.day)
        return result
    if samples_listing.day:
        raise RuntimeError(
            f"{day} has a data.parquet and also loose hour or part files. That should be "
            "impossible (hourly compaction refuses a compacted day); inspect before merging"
        )

    first_hour = max(_day_start_ms(day), STREAM_START_MS)
    for hour in range(first_hour, _day_start_ms(day) + 24 * prom.HOUR_MS, prom.HOUR_MS):
        if hour not in samples_listing.hours:
            compact_hour(lake, client, hour, now_ms)
            result.hours_compacted_here.append(datetime.fromtimestamp(hour / 1000, UTC).strftime("%H:00"))

    samples_listing = list_day(lake, SAMPLES_PREFIX, day)
    series_listing = list_day(lake, SERIES_PREFIX, day)
    hour_keys = [samples_listing.hours[h] for h in sorted(samples_listing.hours)]
    result.hours_merged = len(hour_keys)

    with tempfile.TemporaryDirectory() as scratch:
        local = Path(scratch) / "data.parquet"
        writer = None
        try:
            for key in hour_keys:
                table = lakeio.read_strict(lake, key)
                if table is None or not table.num_rows:
                    continue
                table = table.cast(prom.SAMPLE_SCHEMA)
                result.rows_in_hours += table.num_rows
                if writer is None:
                    writer = pq.ParquetWriter(local, prom.SAMPLE_SCHEMA, compression="zstd")
                writer.write_table(table, row_group_size=1_000_000)
        finally:
            if writer is not None:
                writer.close()
        if writer is not None:
            result.bytes_written += lakeio.put_file(lake, day_key(SAMPLES_PREFIX, day), local)
            result.rows = pq.ParquetFile(local).metadata.num_rows

    series = dedupe_series(_read_all(lake, [series_listing.hours[h] for h in sorted(series_listing.hours)]))
    result.series = series.num_rows
    if series.num_rows:
        result.bytes_written += lake.write_table(day_key(SERIES_PREFIX, day), series)

    if result.rows != result.rows_in_hours:
        raise RuntimeError(
            f"{day}: the merged file holds {result.rows} rows, the hours {result.rows_in_hours}. "
            "Hour files were left in place."
        )
    lakeio.delete_keys(lake, hour_keys)
    lakeio.delete_keys(lake, [series_listing.hours[h] for h in sorted(series_listing.hours)])
    return result


# --------------------------------------------------------------------------------
# assets
# --------------------------------------------------------------------------------


def _hours_of(context: AssetExecutionContext) -> list[int]:
    window = context.partition_time_window
    start, end = prom.ms_of(window.start), prom.ms_of(window.end)
    return list(range(start, end, prom.HOUR_MS))


@asset(
    partitions_def=hourly_partitions,
    group_name="prometheus",
    kinds={"python", "prometheus"},
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    # A backfill of history runs several hours per run: the run queue has ten slots for
    # the whole instance, and the AI platform's automations queue behind these.
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=24),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_MATCHES_PROMETHEUS, asset="prometheus_samples_raw",
            description=(
                "Every scrape of the hour is present (its `up` samples match Prometheus's "
                "count exactly) and each job holds the samples its scrapes delivered, within "
                "[rate withheld] ([rate withheld] for cadvisor, which repeats its own timestamps). Short jobs are "
                "re-read first; this fails only when Prometheus no longer holds what the "
                "stream missed."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_NO_DUPLICATES, asset="prometheus_samples_raw",
            description="No two rows share a series and a timestamp.",
        ),
        AssetCheckSpec(
            name=CHECK_STREAM_FRESH, asset="prometheus_samples_raw", blocking=True,
            description=(
                "For a recent hour, the sensor has written a part within the last ten "
                "minutes. Blocking, so a stopped stream fails the run and opens an "
                "incident: a sensor's own errors reach no failure sensor. Not evaluated "
                "for older hours, which are backfills."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_JOBS_DOCUMENTED, asset="prometheus_samples_raw",
            description=(
                "Every job in the hour is described in parsing/prometheus.py JOB_NOTES. "
                "An undocumented job is still ingested; this only names it."
            ),
        ),
    ],
    description=(
        "Every raw sample Prometheus scraped, one hour at a time: timestamp, series id, "
        "metric, job, instance, value. Merges the sensor's minute parts and re-reads from "
        "Prometheus any job the stream holds incompletely. Lake only."
    ),
)
def prometheus_samples_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    client = client_from_env()
    now_ms = _now_ms()
    results = [compact_hour(lake, client, hour, now_ms) for hour in _hours_of(context)]
    for result in results:
        context.log.info(
            "%s rows=%d series=%d parts=%d reread=%s duplicates=%d%s", result.label,
            result.rows, result.series, result.parts_merged, result.reread or "-",
            result.duplicates_removed, " (already in the day file)" if result.already_in_day_file else "",
        )

    short = {r.label: r.short for r in results if r.short}
    duplicates = sum(r.duplicates_removed for r in results)
    jobs = sorted({job for r in results for job in r.expected})
    undocumented = [job for job in jobs if job not in prom.JOB_NOTES]

    latest_hour_end = max(r.hour_ms for r in results) + prom.HOUR_MS
    recent = latest_hour_end >= now_ms - 3 * prom.HOUR_MS
    position = stream_position(lake, now_ms) if recent else None
    behind_ms = now_ms - position if position is not None else None
    fresh = (not recent) or (behind_ms is not None and behind_ms <= STALE_AFTER_MS + LAG_MS)

    detail = "\n".join(
        f"{r.label}  rows {r.rows:>9,}  parts {r.parts_merged:>3}  reread {','.join(r.reread) or '-'}"
        for r in results
    )
    return MaterializeResult(
        metadata={
            "hours": len(results),
            "rows": sum(r.rows for r in results),
            "series": max((r.series for r in results), default=0),
            "bytes_written": sum(r.bytes_written for r in results),
            "parts_merged": sum(r.parts_merged for r in results),
            "hours_reread": sum(1 for r in results if r.reread),
            "native_histogram_series_skipped": sum(r.native_histogram_series for r in results),
            "detail": MetadataValue.text(detail),
        },
        check_results=[
            AssetCheckResult(
                check_name=CHECK_MATCHES_PROMETHEUS, passed=not short,
                severity=AssetCheckSeverity.WARN,
                metadata={"short_hours": MetadataValue.text(json.dumps(short, indent=1) if short else "none")},
            ),
            AssetCheckResult(
                check_name=CHECK_NO_DUPLICATES, passed=True,
                metadata={"duplicates_removed_while_merging": duplicates},
            ),
            AssetCheckResult(
                check_name=CHECK_STREAM_FRESH, passed=fresh,
                severity=AssetCheckSeverity.ERROR,
                metadata={
                    "evaluated": recent,
                    "seconds_behind": round(behind_ms / 1000) if behind_ms is not None else -1,
                },
                description=None if fresh else (
                    "The prometheus_stream sensor has not written a part recently. Check it "
                    "is running in the Dagster UI (Automation) and read its tick log. The "
                    "lake stays complete through the hourly re-read; only freshness is lost."
                ),
            ),
            AssetCheckResult(
                check_name=CHECK_JOBS_DOCUMENTED, passed=not undocumented,
                severity=AssetCheckSeverity.WARN,
                metadata={"jobs": ", ".join(jobs), "undocumented": ", ".join(undocumented) or "none"},
            ),
        ],
    )


@asset(
    partitions_def=daily_partitions,
    group_name="prometheus",
    kinds={"python", "prometheus"},
    deps=[prometheus_samples_raw],
    retry_policy=RetryPolicy(max_retries=2, delay=60),
    backfill_policy=BackfillPolicy.single_run(),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_DAY_COMPLETE, asset="prometheus_samples_compacted",
            description="The day file holds exactly the rows of the hour files it replaced.",
        ),
    ],
    description=(
        "Merges a finished day's hour files into one data.parquet for samples and one for "
        "series, so a query over a past day opens one object instead of twenty-four."
    ),
)
def prometheus_samples_compacted(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    client = client_from_env()
    now_ms = _now_ms()
    window = context.partition_time_window
    day = window.start.date()
    results = []
    while day < window.end.date():
        results.append(compact_day(lake, client, day, now_ms))
        day += timedelta(days=1)
    for r in results:
        context.log.info("%s rows=%d series=%d hours=%d compacted_here=%s%s", r.day, r.rows, r.series,
                         r.hours_merged, r.hours_compacted_here or "-",
                         " (already compacted)" if r.already_compacted else "")
    return MaterializeResult(
        metadata={
            "days": len(results),
            "rows": sum(r.rows for r in results),
            "bytes_written": sum(r.bytes_written for r in results),
            "hours_compacted_here": sum(len(r.hours_compacted_here) for r in results),
        },
        check_results=[AssetCheckResult(
            check_name=CHECK_DAY_COMPLETE,
            passed=all(r.already_compacted or r.rows == r.rows_in_hours for r in results),
            metadata={"rows_in_day_files": sum(r.rows for r in results),
                      "rows_in_hour_files": sum(r.rows_in_hours for r in results)},
        )],
    )


@asset(
    partitions_def=daily_partitions,
    group_name="prometheus",
    kinds={"python", "prometheus"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    description=(
        "What each metric is: its type (counter, gauge, histogram, summary), help text "
        "and unit, as Prometheus reported them on the day. The type is what tells a "
        "query whether a column needs a rate."
    ),
)
def prometheus_metadata_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    captured_at = datetime.now(UTC)
    table = prom.metadata_table(client_from_env().metadata(), captured_at)
    key = f"{day_prefix(METADATA_PREFIX, date.fromisoformat(context.partition_key))}/data.parquet"
    lake.delete_prefix(day_prefix(METADATA_PREFIX, date.fromisoformat(context.partition_key)))
    written = lake.write_table(key, table)
    types = table.group_by("type").aggregate([("metric", "count_distinct")]).to_pylist()
    return MaterializeResult(metadata={
        "metrics": len(set(table.column("metric").to_pylist())),
        "by_type": MetadataValue.json({row["type"]: row["metric_count_distinct"] for row in types}),
        "bytes_written": written,
    })


# --------------------------------------------------------------------------------
# jobs, schedules, sensor
# --------------------------------------------------------------------------------

prometheus_hourly_job = define_asset_job(
    name="prometheus_hourly", selection=[prometheus_samples_raw],
    description="Compact one hour of Prometheus samples, re-reading whatever the stream missed.",
)
prometheus_daily_job = define_asset_job(
    name="prometheus_daily", selection=[prometheus_samples_compacted],
    description="Merge a finished day of Prometheus hour files into one object.",
)
prometheus_metadata_job = define_asset_job(
    name="prometheus_metadata", selection=[prometheus_metadata_raw],
    description="Snapshot what every Prometheus metric is.",
)


# RUNNING by default, unlike the daily source schedules. The stream writes parts every
# minute; a compactor left stopped is not a pause, it is an unbounded pile of small
# objects and a lake that is complete only by accident.
@schedule(job=prometheus_hourly_job, cron_schedule="25 * * * *",
          default_status=DefaultScheduleStatus.RUNNING,
          description="The previous hour at :25, once the stream can no longer write to it.")
def prometheus_hourly_schedule(context: ScheduleEvaluationContext):
    hour = context.scheduled_execution_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    key = hour.strftime("%Y-%m-%d-%H:%M")
    yield RunRequest(run_key=f"prometheus-hour-{key}", partition_key=key)


@schedule(job=prometheus_daily_job, cron_schedule="20 0 * * *",
          default_status=DefaultScheduleStatus.RUNNING,
          description="Yesterday's hour files merged at 00:20 UTC, after its last hour compacted at 23:25.")
def prometheus_daily_schedule(context: ScheduleEvaluationContext):
    day = (context.scheduled_execution_time.date() - timedelta(days=1)).isoformat()
    yield RunRequest(run_key=f"prometheus-day-{day}", partition_key=day)


@schedule(job=prometheus_metadata_job, cron_schedule="10 3 * * *",
          default_status=DefaultScheduleStatus.RUNNING,
          description="Today's metric metadata at 03:10 UTC. Today's: a snapshot describes now.")
def prometheus_metadata_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date().isoformat()
    yield RunRequest(run_key=f"prometheus-metadata-{day}", partition_key=day)


@sensor(
    name="prometheus_stream",
    job=prometheus_hourly_job,
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "The stream. Every minute, reads every sample Prometheus stored since the last "
        "tick (up to one minute ago) and writes it to the lake as a part. After an outage "
        "it reads the last fifteen minutes and hands the missed hours to prometheus_hourly."
    ),
)
def prometheus_stream(context: SensorEvaluationContext, lake: LakeStorage):
    tick = stream_tick(lake, client_from_env(), context.cursor, _now_ms())
    requests = [
        RunRequest(
            run_key=f"prometheus-repair-{hour}",
            partition_key=datetime.fromtimestamp(hour / 1000, UTC).strftime("%Y-%m-%d-%H:%M"),
            tags={"homelab/trigger": "prometheus_stream_repair"},
        )
        for hour in tick.repairs
    ]
    if requests:
        context.log.info("%s; repairing %d missed hours, %d still queued", tick.message,
                         len(requests), len(tick.pending))
        return SensorResult(run_requests=requests, cursor=tick.cursor())
    return SensorResult(skip_reason=SkipReason(tick.message), cursor=tick.cursor())


ASSETS = [prometheus_samples_raw, prometheus_samples_compacted, prometheus_metadata_raw]
JOBS = [prometheus_hourly_job, prometheus_daily_job, prometheus_metadata_job]
SCHEDULES = [prometheus_hourly_schedule, prometheus_daily_schedule, prometheus_metadata_schedule]
SENSORS = [prometheus_stream]
