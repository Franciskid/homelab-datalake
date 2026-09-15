"""Raw samples out of Prometheus, and the shape they land in.

Three decisions here are the design, not detail.

**Raw samples, never `query_range`.** `query_range` evaluates an expression at a fixed
step and returns interpolated values -- a number Prometheus computed, not one it
scraped. An *instant* query of a bare range selector, `{job="node"}[300000ms]`, returns
every stored sample in the window with its own timestamp, which is what a lake that
keeps data for ever must hold. Prometheus 3 made range selectors left-open, so the
query is evaluated one millisecond before the window's end: `(start - 1ms, end - 1ms]`
is exactly `[start, end)` in whole milliseconds, which is how every window here is cut.
Adjacent windows therefore never share a sample, and dedupe on `(series_id, ts)` is a
safety net rather than a necessity.

**Labels are stored once per series, not on every sample.** Measured on primary on
2026-09-14: 1.95 bytes per sample with a series id, 9.4 bytes with the label set
repeated on every row. The samples carry `series_id`, `metric`, `job` and `instance` --
enough to filter without a join -- and `prometheus_series` maps the id to its labels.

**Every job is ingested.** `JOB_NOTES` documents the jobs known on 2026-09-15; a job
Prometheus reports that is not in it is still landed, and a check names it. An
allowlist would have been the silent kind of deny-by-default that lost the camera's
`counts` field for a month (see docs/DATA_COMPLETENESS_PLAN.md).
"""

from __future__ import annotations

import hashlib
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, date, datetime

import pyarrow as pa

HOUR_MS = 3_600_000
MINUTE_MS = 60_000

# How far below `sample_count` a job's hour may land before it counts as short. Missing
# scrapes are caught exactly by `scrape_count` regardless; this only absorbs samples
# Prometheus counted and then dropped. cadvisor: measured [rate withheld] on 2026-09-15.
DEFAULT_TOLERANCE = 0.001
TOLERANCE: dict[str, float] = {"cadvisor": 0.05}


def tolerance(job: str) -> float:
    return TOLERANCE.get(job, DEFAULT_TOLERANCE)

# What each job is, as of the day this was written. Documentation, not a filter.
JOB_NOTES: dict[str, str] = {
    "node": "Host CPU, memory, disk, network and temperatures from node-exporter, every [duration withheld].",
    "cadvisor": "Per-container CPU, memory, network and filesystem from cAdvisor, every [duration withheld].",
    "gpu": "NVIDIA GPU utilisation, memory, power, temperature and throttling, every [duration withheld].",
    "ai-litellm": "The LiteLLM gateway: requests, failures, latency histograms, budgets.",
    "ai-platform-api": "The AI platform API process: up, build info, resident memory.",
    "status-page": (
        "Sentinel's view of Secondary and Tertiary through the status page: UP/DOWN state, "
        "incidents, cpu, memory and disk. Added 2026-09-15."
    ),
    "prometheus": "Prometheus's own health: scrape durations, TSDB size, rule evaluations.",
    # Retired on 2026-09-15. They scraped node-exporter and the GPU exporter a second
    # time (every [duration withheld] and every [duration withheld]). Kept because before that date they are real,
    # higher-resolution history, and a lake that keeps data for ever keeps that too.
    "node-exporter": "Retired 2026-09-15: node-exporter scraped a second time, every [duration withheld].",
    "nvidia_smi_exporter": "Retired 2026-09-15: the GPU exporter scraped a second time, every [duration withheld].",
}

SAMPLE_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("ts", pa.timestamp("ms", tz="UTC")),
    ("series_id", pa.int64()),
    ("metric", pa.string()),
    ("job", pa.string()),
    ("instance", pa.string()),
    ("value", pa.float64()),
])

SERIES_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("series_id", pa.int64()),
    ("metric", pa.string()),
    ("job", pa.string()),
    ("instance", pa.string()),
    ("labels", pa.map_(pa.string(), pa.string())),
    ("first_seen", pa.timestamp("ms", tz="UTC")),
    ("last_seen", pa.timestamp("ms", tz="UTC")),
])

METADATA_SCHEMA = pa.schema([
    ("dt", pa.date32()),
    ("metric", pa.string()),
    ("type", pa.string()),
    ("help", pa.string()),
    ("unit", pa.string()),
    ("captured_at", pa.timestamp("ms", tz="UTC")),
])


class PrometheusError(RuntimeError):
    """Prometheus answered, and the answer was an error."""


def series_id(labels: dict[str, str]) -> int:
    """A stable 64-bit id for one label set, `__name__` included.

    blake2b over the sorted pairs, so the id does not depend on the order the API
    happened to return the labels in. Signed, because Parquet and DuckDB have no
    unsigned 64-bit type that every reader agrees on.
    """
    canonical = json.dumps(sorted(labels.items()), separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.blake2b(canonical.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def hour_floor(ms: int) -> int:
    return ms - ms % HOUR_MS


def day_of(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000, UTC).date()


def ms_of(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def windows(start_ms: int, end_ms: int, width_ms: int) -> list[tuple[int, int]]:
    """`[start, end)` cut into consecutive half-open windows of at most `width_ms`."""
    out = []
    cursor = start_ms
    while cursor < end_ms:
        out.append((cursor, min(cursor + width_ms, end_ms)))
        cursor += width_ms
    return out


def _selector(job: str) -> str:
    escaped = job.replace("\\", "\\\\").replace('"', '\\"')
    return f'{{job="{escaped}"}}'


@dataclass
class Batch:
    """Samples as columns, plus the label set of every series they belong to."""

    ts: list[int] = field(default_factory=list)
    series_id: list[int] = field(default_factory=list)
    metric: list[str] = field(default_factory=list)
    job: list[str] = field(default_factory=list)
    instance: list[str] = field(default_factory=list)
    value: list[float] = field(default_factory=list)
    # series_id -> (labels, first ts, last ts)
    series: dict[int, tuple[dict[str, str], int, int]] = field(default_factory=dict)
    # Native histograms carry buckets, not a float, and this table has nowhere to put
    # them. None are scraped on primary today; if one appears it is counted, not lost
    # silently, and a check reports it.
    native_histogram_series: int = 0

    def __len__(self) -> int:
        return len(self.ts)

    def add_result(self, results: list[dict], start_ms: int, end_ms: int) -> None:
        for result in results:
            labels = dict(result.get("metric") or {})
            if "histograms" in result and "values" not in result:
                self.native_histogram_series += 1
                continue
            values = result.get("values") or []
            if not values:
                continue
            sid = series_id(labels)
            name = labels.get("__name__", "")
            job = labels.get("job", "")
            instance = labels.get("instance", "")
            kept = 0
            first = last = None
            for raw_ts, raw_value in values:
                ts = round(float(raw_ts) * 1000)
                if not (start_ms <= ts < end_ms):
                    continue
                self.ts.append(ts)
                self.value.append(float(raw_value))
                kept += 1
                first = ts if first is None else min(first, ts)
                last = ts if last is None else max(last, ts)
            if not kept:
                continue
            self.series_id.extend([sid] * kept)
            self.metric.extend([name] * kept)
            self.job.extend([job] * kept)
            self.instance.extend([instance] * kept)
            known = self.series.get(sid)
            if known is None:
                self.series[sid] = (labels, first, last)
            else:
                self.series[sid] = (known[0], min(known[1], first), max(known[2], last))

    def extend(self, other: Batch) -> None:
        self.ts.extend(other.ts)
        self.series_id.extend(other.series_id)
        self.metric.extend(other.metric)
        self.job.extend(other.job)
        self.instance.extend(other.instance)
        self.value.extend(other.value)
        for sid, (labels, first, last) in other.series.items():
            known = self.series.get(sid)
            self.series[sid] = (
                (labels, first, last) if known is None
                else (known[0], min(known[1], first), max(known[2], last))
            )
        self.native_histogram_series += other.native_histogram_series

    def split_by_hour(self) -> dict[int, Batch]:
        """One batch per UTC hour the samples fall in. Series follow their samples."""
        hours: dict[int, Batch] = {}
        for index, ts in enumerate(self.ts):
            hour = hour_floor(ts)
            target = hours.get(hour)
            if target is None:
                target = hours[hour] = Batch()
            sid = self.series_id[index]
            target.ts.append(ts)
            target.series_id.append(sid)
            target.metric.append(self.metric[index])
            target.job.append(self.job[index])
            target.instance.append(self.instance[index])
            target.value.append(self.value[index])
            known = target.series.get(sid)
            if known is None:
                target.series[sid] = (self.series[sid][0], ts, ts)
            else:
                target.series[sid] = (known[0], min(known[1], ts), max(known[2], ts))
        return hours

    def samples_table(self) -> pa.Table:
        days = [day_of(ts) for ts in self.ts] if self.ts else []
        return pa.table(
            {
                "dt": pa.array(days, pa.date32()),
                "ts": pa.array(self.ts, pa.timestamp("ms", tz="UTC")),
                "series_id": pa.array(self.series_id, pa.int64()),
                "metric": pa.array(self.metric, pa.string()),
                "job": pa.array(self.job, pa.string()),
                "instance": pa.array(self.instance, pa.string()),
                "value": pa.array(self.value, pa.float64()),
            },
            schema=SAMPLE_SCHEMA,
        )

    def series_table(self, only: set[int] | None = None) -> pa.Table:
        rows = [
            (sid, labels, first, last)
            for sid, (labels, first, last) in self.series.items()
            if only is None or sid in only
        ]
        return series_rows_table(rows)


def series_rows_table(rows: list[tuple[int, dict[str, str], int, int]]) -> pa.Table:
    return pa.table(
        {
            "dt": pa.array([day_of(first) for _, _, first, _ in rows], pa.date32()),
            "series_id": pa.array([sid for sid, *_ in rows], pa.int64()),
            "metric": pa.array([labels.get("__name__", "") for _, labels, *_ in rows]),
            "job": pa.array([labels.get("job", "") for _, labels, *_ in rows]),
            "instance": pa.array([labels.get("instance", "") for _, labels, *_ in rows]),
            "labels": pa.array(
                [
                    [(k, v) for k, v in sorted(labels.items()) if k != "__name__"]
                    for _, labels, *_ in rows
                ],
                pa.map_(pa.string(), pa.string()),
            ),
            "first_seen": pa.array([first for *_, first, _ in rows], pa.timestamp("ms", tz="UTC")),
            "last_seen": pa.array([last for *_, last in rows], pa.timestamp("ms", tz="UTC")),
        },
        schema=SERIES_SCHEMA,
    )


def metadata_table(metadata: dict[str, list[dict]], captured_at: datetime) -> pa.Table:
    """One row per distinct (metric, type, help, unit). A metric exposed by two
    targets with different help text keeps both rows rather than picking one."""
    seen = set()
    rows = []
    for metric in sorted(metadata):
        for entry in metadata[metric] or []:
            key = (metric, entry.get("type", ""), entry.get("help", ""), entry.get("unit", ""))
            if key in seen:
                continue
            seen.add(key)
            rows.append(key)
    day = captured_at.astimezone(UTC).date()
    return pa.table(
        {
            "dt": pa.array([day] * len(rows), pa.date32()),
            "metric": pa.array([r[0] for r in rows]),
            "type": pa.array([r[1] for r in rows]),
            "help": pa.array([r[2] for r in rows]),
            "unit": pa.array([r[3] for r in rows]),
            "captured_at": pa.array([captured_at] * len(rows), pa.timestamp("ms", tz="UTC")),
        },
        schema=METADATA_SCHEMA,
    )


class PrometheusClient:
    """The four HTTP API calls the lake needs. Plain urllib: no client library to
    keep in step with a server that has already moved a major version once."""

    def __init__(self, base_url: str, timeout: float = 120.0):
        if not base_url.startswith(("http://", "https://")):
            # urlopen also speaks file:// and ftp://. A misconfigured URL must fail,
            # not turn into a local file read.
            raise ValueError(f"PROMETHEUS_URL must be http(s), got {base_url!r}")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, params: dict[str, str]) -> dict | list:
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})  # noqa: S310
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                body = json.load(response)
        except urllib.error.HTTPError as error:
            # Prometheus answers a bad or refused query with a JSON body naming the reason;
            # the bare "HTTP Error 422" says nothing about which limit or which query.
            try:
                body = json.load(error)
            except ValueError:
                raise PrometheusError(f"{path}: HTTP {error.code}") from error
            raise PrometheusError(
                f"{path} {params.get('query', '')!r}: {body.get('errorType')}: {body.get('error')}"
            ) from error
        if body.get("status") != "success":
            raise PrometheusError(f"{path}: {body.get('errorType')}: {body.get('error')}")
        return body["data"]

    def jobs(self, start_ms: int, end_ms: int) -> list[str]:
        """The jobs with data in `[start, end)` -- not every job the TSDB ever held."""
        data = self._get("/api/v1/label/job/values", {
            "start": f"{start_ms / 1000:.3f}", "end": f"{(end_ms - 1) / 1000:.3f}",
        })
        return sorted(data)

    def raw_samples(self, job: str, start_ms: int, end_ms: int) -> list[dict]:
        width = end_ms - start_ms
        return self._get("/api/v1/query", {
            "query": f"{_selector(job)}[{width}ms]",
            "time": f"{(end_ms - 1) / 1000:.3f}",
        })["result"]

    def _scalar(self, query: str, end_ms: int) -> int:
        result = self._get("/api/v1/query", {"query": query, "time": f"{(end_ms - 1) / 1000:.3f}"})["result"]
        if not result:
            return 0
        value = float(result[0]["value"][1])
        return 0 if math.isnan(value) else int(value)

    def sample_count(self, job: str, start_ms: int, end_ms: int) -> int:
        """How many samples `job`'s scrapes delivered in `[start, end)`.

        Not `count_over_time({job="x"}[1h])`: that drops every series' name, and two
        series that differ only by name then collide ("vector cannot contain metrics
        with the same labelset") -- which every exporter on primary has. Instead,
        Prometheus's own per-scrape record: the samples each scrape kept after
        relabelling, plus the five series it writes about every scrape itself (up,
        scrape_duration_seconds, scrape_samples_scraped,
        scrape_samples_post_metric_relabeling, scrape_series_added).

        Measured against a full raw pull of two hours, per job: exact for every job but
        cadvisor, where it over-counts by ~2.1%. cAdvisor stamps samples with its own
        timestamps and repeats them, and Prometheus drops the repeats after counting
        them. See `TOLERANCE`.
        """
        width = f"[{end_ms - start_ms}ms]"
        selector = _selector(job)[1:-1]
        return self._scalar(
            f"sum(sum_over_time(scrape_samples_post_metric_relabeling{{{selector}}}{width}))"
            f" + 5 * sum(count_over_time(up{{{selector}}}{width}))",
            end_ms,
        )

    def scrape_count(self, job: str, start_ms: int, end_ms: int) -> int:
        """How many scrapes of `job` happened in `[start, end)`: its `up` samples.

        Exact for every job, because `up` is written by Prometheus once per scrape and
        never carries an exporter's timestamp. A lake hour holding fewer `up` samples
        than this is missing whole scrapes, whatever the total count says.
        """
        return self._scalar(
            f"sum(count_over_time(up{{{_selector(job)[1:-1]}}}[{end_ms - start_ms}ms]))", end_ms)

    def metadata(self) -> dict[str, list[dict]]:
        return self._get("/api/v1/metadata", {})

    def pull(self, job: str, start_ms: int, end_ms: int, window_ms: int = 5 * MINUTE_MS) -> Batch:
        """Every sample of `job` in `[start, end)`, fetched in windows so no single
        response grows past what one request should carry (cadvisor alone is [volume withheld]
        of JSON per minute)."""
        batch = Batch()
        for window_start, window_end in windows(start_ms, end_ms, window_ms):
            batch.add_result(self.raw_samples(job, window_start, window_end), window_start, window_end)
        return batch
