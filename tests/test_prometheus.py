"""The Prometheus stream: windows, series identity, parts, and compaction against the source.

A fake Prometheus stands in for the real one. It answers the three calls the lake makes
from one list of samples per job, including Prometheus 3's left-open range selector, so
a window cut off by one millisecond fails here rather than as a missing sample in the lake.
"""

from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from homelab_datalake import lakeio
from homelab_datalake.assets import prometheus as stream
from homelab_datalake.parsing import prometheus as prom
from homelab_datalake.resources import LakeStorage

HOUR = prom.HOUR_MS
T0 = prom.ms_of(datetime(2026, 9, 14, 10, tzinfo=UTC))


class FakePrometheus(prom.PrometheusClient):
    """Series of samples every [duration withheld], per job, answering like the HTTP API does."""

    def __init__(self, series: dict[str, list[dict]]):
        super().__init__("http://prometheus.test")
        self.series = series  # job -> [{"metric": labels, "samples": [(ms, value)]}]
        self.pulled: list[tuple[str, int, int]] = []

    def jobs(self, start_ms, end_ms):
        return sorted(job for job, items in self.series.items()
                      if any(start_ms <= ts < end_ms for s in items for ts, _ in s["samples"]))

    def raw_samples(self, job, start_ms, end_ms):
        # The real call evaluates `[end - start]` at end - 1 ms, left-open: (start - 1, end - 1].
        self.pulled.append((job, start_ms, end_ms))
        low, high = start_ms - 1, end_ms - 1
        out = []
        for item in self.series.get(job, []):
            values = [[ts / 1000, str(v)] for ts, v in item["samples"] if low < ts <= high]
            if values:
                out.append({"metric": item["metric"], "values": values})
        return out

    def sample_count(self, job, start_ms, end_ms):
        return sum(1 for item in self.series.get(job, [])
                   for ts, _ in item["samples"] if start_ms <= ts < end_ms)

    def scrape_count(self, job, start_ms, end_ms):
        return sum(1 for item in self.series.get(job, []) if item["metric"]["__name__"] == "up"
                   for ts, _ in item["samples"] if start_ms <= ts < end_ms)


def _series(job, name, start_ms, end_ms, step_ms=30_000, **labels):
    return {
        "metric": {"__name__": name, "job": job, "instance": f"{job}:9100", **labels},
        "samples": [(ts, float(i)) for i, ts in enumerate(range(start_ms, end_ms, step_ms))],
    }


@pytest.fixture
def lake(tmp_path):
    return LakeStorage(local_root=str(tmp_path))


def _fake(start=T0, end=T0 + 2 * HOUR):
    return FakePrometheus({
        "node": [_series("node", "node_load1", start, end),
                 _series("node", "node_cpu_seconds_total", start, end, mode="idle")],
        "gpu": [_series("gpu", "nvidia_smi_utilization_gpu_ratio", start, end, uuid="GPU-1")],
    })


# ---------------------------------------------------------------------------- identity


def test_series_id_does_not_depend_on_label_order():
    a = {"__name__": "up", "job": "node", "instance": "x"}
    b = {"instance": "x", "job": "node", "__name__": "up"}
    assert prom.series_id(a) == prom.series_id(b)
    assert prom.series_id(a) != prom.series_id({**a, "instance": "y"})


def test_windows_are_half_open_and_cover_everything():
    cut = prom.windows(0, 1_000, 300)
    assert cut == [(0, 300), (300, 600), (600, 900), (900, 1_000)]


def test_a_batch_keeps_only_its_window_and_parses_special_values():
    batch = prom.Batch()
    batch.add_result([{"metric": {"__name__": "m", "job": "j"},
                       "values": [[0.999, "1"], [1.0, "NaN"], [1.5, "+Inf"], [2.0, "3"]]}], 1_000, 2_000)
    assert batch.ts == [1_000, 1_500]
    assert str(batch.value[0]) == "nan" and batch.value[1] == float("inf")


def test_a_native_histogram_is_counted_not_dropped_silently():
    batch = prom.Batch()
    batch.add_result([{"metric": {"__name__": "h"}, "histograms": [[1, {}]]}], 0, 10_000)
    assert len(batch) == 0 and batch.native_histogram_series == 1


def test_split_by_hour_moves_series_with_their_samples():
    batch = prom.Batch()
    batch.add_result([{"metric": {"__name__": "m", "job": "j"},
                       "values": [[(HOUR - 1_000) / 1000, "1"], [(HOUR + 1_000) / 1000, "2"]]}], 0, 2 * HOUR)
    hours = batch.split_by_hour()
    assert sorted(hours) == [0, HOUR]
    assert all(len(h.series) == 1 for h in hours.values())


def test_labels_live_in_the_series_table_not_on_samples():
    batch = _fake().pull("node", T0, T0 + 10 * 60_000)
    samples, series = batch.samples_table(), batch.series_table()
    assert "labels" not in samples.column_names
    assert series.num_rows == 2
    labels = {tuple(sorted(dict(row["labels"]).items())) for row in series.to_pylist()}
    assert (("instance", "node:9100"), ("job", "node"), ("mode", "idle")) in labels


def test_every_job_on_primary_is_documented():
    for job in ("node", "cadvisor", "gpu", "ai-litellm", "ai-platform-api", "status-page", "prometheus",
                "node-exporter", "nvidia_smi_exporter"):
        assert job in prom.JOB_NOTES


def test_a_non_http_url_is_refused():
    with pytest.raises(ValueError, match="http"):
        prom.PrometheusClient("file:///etc/passwd")


# ---------------------------------------------------------------------------- the stream


def test_a_tick_writes_parts_and_advances_the_cursor(lake):
    fake = _fake()
    now = T0 + 11 * 60_000
    tick = stream.stream_tick(lake, fake, None, now, emitted={})
    assert tick.samples > 0 and tick.end_ms == now - stream.LAG_MS
    start, _ = stream.parse_cursor(tick.cursor())
    assert start == tick.end_ms
    listing = stream.list_day(lake, stream.SAMPLES_PREFIX, date(2026, 9, 14))
    assert len(listing.parts) == 1


def test_consecutive_ticks_neither_overlap_nor_leave_a_gap(lake):
    fake = _fake()
    emitted: dict = {}
    cursor = json_cursor = None
    for minute in range(5, 20, 3):
        tick = stream.stream_tick(lake, fake, json_cursor, T0 + minute * 60_000, emitted=emitted)
        json_cursor = tick.cursor()
        cursor = tick.end_ms
    tables = [lakeio.read_strict(lake, k) for k in
              stream.list_day(lake, stream.SAMPLES_PREFIX, date(2026, 9, 14)).parts.values()]
    rows = sum(t.num_rows for t in tables)
    expected = sum(fake.sample_count(job, T0, cursor) for job in ("node", "gpu"))
    assert rows == expected


def test_series_are_written_once_per_hour_by_a_long_lived_process(lake):
    fake = _fake()
    emitted: dict = {}
    first = stream.stream_tick(lake, fake, None, T0 + 5 * 60_000, emitted=emitted)
    second = stream.stream_tick(lake, fake, first.cursor(), T0 + 8 * 60_000, emitted=emitted)
    assert first.series == 3 and second.series == 0


def test_after_an_outage_the_tick_reads_fifteen_minutes_and_queues_the_missed_hours(lake):
    fake = _fake(end=T0 + 6 * HOUR)
    cursor = json_cursor = stream.Tick(start_ms=T0, end_ms=T0 + 10 * 60_000).cursor()
    tick = stream.stream_tick(lake, fake, cursor, T0 + 4 * HOUR + 30 * 60_000, emitted={})
    assert tick.end_ms - tick.start_ms == stream.MAX_TICK_MS
    queued = tick.repairs + tick.pending
    assert queued == [T0, T0 + HOUR, T0 + 2 * HOUR, T0 + 3 * HOUR]
    assert len(tick.repairs) == stream.REPAIRS_PER_TICK
    later = stream.stream_tick(lake, fake, tick.cursor(), tick.end_ms + stream.LAG_MS + 60_000, emitted={})
    assert later.repairs == [T0 + 2 * HOUR, T0 + 3 * HOUR] and not later.pending
    assert json_cursor


# ---------------------------------------------------------------------------- compaction


def test_compacting_an_hour_merges_parts_and_deletes_them(lake):
    fake = _fake()
    cursor = None
    for minute in range(10, 75, 10):
        cursor = stream.stream_tick(lake, fake, cursor, T0 + minute * 60_000, emitted={}).cursor()
    result = stream.compact_hour(lake, fake, T0, T0 + 2 * HOUR)
    assert not result.short and result.rows == sum(fake.sample_count(j, T0, T0 + HOUR) for j in ("node", "gpu"))
    listing = stream.list_day(lake, stream.SAMPLES_PREFIX, date(2026, 9, 14))
    assert T0 in listing.hours and not listing.parts_in_hour(T0)
    # Every tick ran as a fresh process (emitted={}), so each part re-wrote the label sets.
    assert result.parts_merged == 7 and result.series == 3


def test_a_part_written_twice_is_deduplicated_on_compaction(lake):
    fake = FakePrometheus({"node": [_series("node", "node_load1", T0, T0 + 10 * 60_000)]})
    batch = fake.pull("node", T0, T0 + 10 * 60_000)
    stream.write_parts(lake, batch, T0, T0 + 10 * 60_000, {})
    stream.write_parts(lake, batch, T0 + 1, T0 + 10 * 60_000, {})  # a retried tick, new object name
    result = stream.compact_hour(lake, fake, T0, T0 + 2 * HOUR)
    assert result.duplicates_removed == len(batch) and not result.reread and result.rows == len(batch)


def test_an_hour_the_stream_never_saw_is_read_back_from_prometheus(lake):
    fake = _fake()
    result = stream.compact_hour(lake, fake, T0, T0 + 2 * HOUR)
    assert result.reread == ["gpu", "node"]
    assert not result.short
    table = pq.read_table(f"{lake.local_root}/{stream.hour_key(stream.SAMPLES_PREFIX, T0)}")
    assert table.num_rows == result.rows


def test_a_short_job_is_reread_and_the_rest_is_kept(lake):
    fake = _fake()
    stream.stream_tick(lake, fake, None, T0 + 16 * 60_000, emitted={})  # only 15 minutes landed
    result = stream.compact_hour(lake, fake, T0, T0 + 2 * HOUR)
    assert set(result.reread) == {"node", "gpu"}
    assert result.landed == result.expected


def test_an_hour_still_open_to_the_stream_is_refused(lake):
    with pytest.raises(ValueError, match="still open"):
        stream.compact_hour(lake, _fake(), T0, T0 + HOUR + 5 * 60_000)


def test_compacting_a_day_merges_every_hour_and_deletes_them(lake):
    day = date(2026, 9, 14)
    start = prom.ms_of(datetime(2026, 9, 14, tzinfo=UTC))
    fake = FakePrometheus({"node": [_series("node", "up", start, start + 24 * HOUR, step_ms=600_000)]})
    result = stream.compact_day(lake, fake, day, start + 30 * HOUR)
    assert result.rows == 144 and result.rows == result.rows_in_hours
    samples = stream.list_day(lake, stream.SAMPLES_PREFIX, day)
    series = stream.list_day(lake, stream.SERIES_PREFIX, day)
    assert samples.day and not samples.hours and series.day and not series.hours
    again = stream.compact_day(lake, fake, day, start + 30 * HOUR)
    assert again.already_compacted and again.rows == 144


def test_an_hour_of_a_compacted_day_is_verified_not_rewritten(lake):
    day = date(2026, 9, 14)
    start = prom.ms_of(datetime(2026, 9, 14, tzinfo=UTC))
    fake = FakePrometheus({"node": [_series("node", "up", start, start + 24 * HOUR, step_ms=600_000)]})
    stream.compact_day(lake, fake, day, start + 30 * HOUR)
    result = stream.compact_hour(lake, fake, start + 5 * HOUR, start + 30 * HOUR)
    assert result.already_in_day_file and result.landed == {"node": 6} and not result.short
    assert result.landed_scrapes == {"node": 6}


def test_a_missing_scrape_is_short_even_inside_the_sample_tolerance(lake):
    """cadvisor may land [rate withheld] under its count; a whole missing scrape still triggers a re-read."""
    series = [_series("cadvisor", "up", T0, T0 + HOUR)] + [
        _series("cadvisor", f"container_metric_{i}", T0, T0 + HOUR) for i in range(99)]
    fake = FakePrometheus({"cadvisor": series})
    batch = fake.pull("cadvisor", T0, T0 + HOUR)
    keep = [i for i, ts in enumerate(batch.ts) if ts != T0 + 30_000]  # one scrape lost
    partial = prom.Batch()
    partial.add_result([], 0, 0)
    for name in ("ts", "series_id", "metric", "job", "instance", "value"):
        setattr(partial, name, [getattr(batch, name)[i] for i in keep])
    partial.series = batch.series
    stream.write_parts(lake, partial, T0, T0 + HOUR, {})
    assert len(partial) > fake.sample_count("cadvisor", T0, T0 + HOUR) * 0.95
    result = stream.compact_hour(lake, fake, T0, T0 + 2 * HOUR)
    assert result.reread == ["cadvisor"] and not result.short


def test_the_stream_position_is_recovered_from_object_names(lake):
    fake = _fake()
    tick = stream.stream_tick(lake, fake, None, T0 + 20 * 60_000, emitted={})
    assert stream.stream_position(lake, T0 + 21 * 60_000) == tick.end_ms


def test_metadata_keeps_one_row_per_distinct_description():
    table = prom.metadata_table(
        {"up": [{"type": "gauge", "help": "h", "unit": ""}, {"type": "gauge", "help": "h", "unit": ""}],
         "x_total": [{"type": "counter", "help": "a", "unit": ""}, {"type": "counter", "help": "b", "unit": ""}]},
        datetime(2026, 9, 15, tzinfo=UTC),
    )
    assert table.num_rows == 3
