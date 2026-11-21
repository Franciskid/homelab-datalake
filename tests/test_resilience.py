"""One failure costs its own lineage, a stall is reported, and long sessions settle.

Written after 14-21 Nov 2026: a dbt test on official statistics failed, the publish
was skipped and every dataset froze at 14 Nov; a dead run then held the analytics
queue for five days; and the owner heard about none of it after the first push.
"""

import datetime
from datetime import UTC
from types import SimpleNamespace

import duckdb
import pytest

from homelab_datalake import alerting, build_report, serving
from homelab_datalake.assets import dbt_marts
from homelab_datalake.assets.jellyfin import (
    _PLAYBACK_SCHEMA,
    PLAYBACK_PREFIX,
    _settle_playback_hour,
)
from homelab_datalake.resources import LakeStorage
from tests.conftest import build_marts

DAY = datetime.date(2026, 11, 21)
NOW = datetime.datetime(2026, 11, 21, 19, 0, tzinfo=UTC)


# --- alerts ---------------------------------------------------------------------

def _fingerprint(job, day, partition="-"):
    return alerting.alert_payload(
        job_name=job, partition=partition, run_id="r", detail="d",
        now=datetime.datetime.combine(day, datetime.time(12), tzinfo=UTC),
    )["alerts"][0]["fingerprint"]


def test_a_critical_failure_opens_a_new_incident_each_day():
    """One fingerprint forever meant one notification, then a week of silence."""
    assert _fingerprint("analytics_hourly", DAY) != _fingerprint("analytics_hourly", DAY - datetime.timedelta(days=1))
    assert _fingerprint("analytics_hourly", DAY) == _fingerprint("analytics_hourly", DAY)


def test_a_source_partition_failure_keeps_one_incident():
    assert _fingerprint("camera_daily", DAY, "2026-11-20") == _fingerprint(
        "camera_daily", DAY + datetime.timedelta(days=3), "2026-11-20"
    )


def test_recovery_resolves_the_dated_incidents_and_the_old_undated_one():
    fingerprints = alerting.recovery_fingerprints("analytics_hourly", "-", DAY)
    assert _fingerprint("analytics_hourly", DAY) in fingerprints
    assert _fingerprint("analytics_hourly", DAY - datetime.timedelta(days=7)) in fingerprints
    assert alerting.legacy_failure_fingerprint("analytics_hourly", "-") in fingerprints
    payload = alerting.resolved_payload(fingerprints, NOW)
    assert {a["status"] for a in payload["alerts"]} == {"resolved"}


def test_a_stale_serving_copy_is_critical_and_dated():
    last = NOW - datetime.timedelta(hours=26)
    alert = alerting.stale_payload(day=DAY, last_publish=last, detail="141 queued", now=NOW)["alerts"][0]
    assert alert["labels"]["severity"] == "critical"
    assert alert["labels"]["alertname"] == "DatalakeServingStale"
    assert "26 hours" in alert["annotations"]["summary"]
    assert "141 queued" in alert["annotations"]["description"]
    assert alert["fingerprint"] != alerting.stale_payload(
        day=DAY + datetime.timedelta(days=1), last_publish=last, detail="", now=NOW
    )["alerts"][0]["fingerprint"]


def test_delivery_without_configuration_reports_and_does_not_raise(monkeypatch):
    monkeypatch.delenv("PLATFORM_ALERT_URL", raising=False)
    logged = []
    log = SimpleNamespace(error=lambda *a: logged.append(a), exception=lambda *a: logged.append(a))
    assert alerting.deliver({"alerts": []}, log) is False
    assert logged


# --- the build report -----------------------------------------------------------

def test_the_report_belongs_to_one_run(tmp_path):
    path = tmp_path / "report.json"
    build_report.reset("run-1", path)
    build_report.record("run-1", [{"stage": "dbt", "name": "t", "detail": "fail: 7"}], ["fct_x"], path)
    assert build_report.read("run-1", path)["quarantined"] == ["fct_x"]
    assert build_report.read("run-2", path) == {"run_id": "run-2", "failures": [], "quarantined": []}


def test_the_summary_says_the_rest_was_published(tmp_path):
    path = tmp_path / "report.json"
    report = build_report.record("r", [{"stage": "load", "name": "raw.x", "detail": "boom"}], ["fct_y"], path)
    text = build_report.summary(report)
    assert "raw.x" in text and "fct_y" in text and "published" in text


# --- dbt failures ---------------------------------------------------------------

class _Invocation:
    def __init__(self, results, manifest, ok=False):
        self._results, self.manifest, self._ok = results, manifest, ok

    def get_artifact(self, name):
        return {"results": self._results}

    def is_successful(self):
        return self._ok


def test_a_failed_test_holds_back_the_model_it_tests():
    manifest = {
        "nodes": {
            "test.p.accepted_values_src": {"name": "accepted_values_src"},
            "model.p.stg_official": {"name": "stg_official"},
            "model.p.fct_jellyfin_playback": {"name": "fct_jellyfin_playback"},
        },
        "parent_map": {"test.p.accepted_values_src": ["model.p.stg_official"]},
    }
    results = [
        {"unique_id": "test.p.accepted_values_src", "status": "fail", "message": "Got 7 results"},
        {"unique_id": "model.p.fct_jellyfin_playback", "status": "success"},
        {"unique_id": "model.p.downstream", "status": "skipped"},
    ]
    failures, held = dbt_marts._failures(_Invocation(results, manifest))
    assert [f["name"] for f in failures] == ["accepted_values_src"]
    assert held == ["stg_official"]


def test_a_model_error_fails_without_holding_anything_back():
    """An errored model kept its previous table, which is safe to serve."""
    manifest = {"nodes": {"model.p.m": {"name": "m"}}, "parent_map": {}}
    failures, held = dbt_marts._failures(
        _Invocation([{"unique_id": "model.p.m", "status": "error", "message": "boom"}], manifest)
    )
    assert failures and held == []


def test_dbt_tests_no_longer_block_the_step():
    """A blocking check is what took the publish, and every dataset, down."""
    for spec in dbt_marts.dbt_marts.check_specs:
        assert not spec.blocking, spec.name


# --- the publish ----------------------------------------------------------------

@pytest.fixture
def warehouse_file(tmp_path):
    path = str(tmp_path / "datalake.duckdb")
    build_marts(path)
    return path


def test_a_held_back_mart_is_served_from_the_previous_copy(warehouse_file, tmp_path):
    target = str(tmp_path / "serving.duckdb")
    serving.publish(warehouse_file, target)
    con = duckdb.connect(warehouse_file)
    before = con.execute("SELECT count(*) FROM marts.fct_jellyfin_playback").fetchone()[0]
    con.execute("INSERT INTO marts.fct_jellyfin_playback SELECT * FROM marts.fct_jellyfin_playback")
    con.execute("CREATE OR REPLACE TABLE marts.fct_llm_call AS SELECT * FROM marts.fct_llm_call")
    con.close()

    serving.publish(warehouse_file, target, keep_previous=frozenset({"fct_jellyfin_playback"}))
    con = duckdb.connect(target, read_only=True)
    assert con.execute("SELECT count(*) FROM marts.fct_jellyfin_playback").fetchone()[0] == before
    con.close()


def test_a_held_back_mart_with_no_previous_copy_refuses_the_publish(warehouse_file, tmp_path):
    """Leaving it out would break the semantic layer; publishing it unchecked could
    serve exactly the rows its test refused. Readers keep the copy they have."""
    with pytest.raises(RuntimeError):
        serving.publish(warehouse_file, str(tmp_path / "s.duckdb"), keep_previous=frozenset({"fct_jellyfin_playback"}))


def test_publishing_with_a_held_back_mart_writes_no_generation_marker(warehouse_file, tmp_path, monkeypatch):
    target = tmp_path / "serving.duckdb"
    serving.publish(warehouse_file, str(target))
    monkeypatch.setattr(serving.rebuild_state, "marts_generation", lambda _path: "gen-1")
    serving.publish_if_changed(warehouse_file, str(target), keep_previous=frozenset({"fct_jellyfin_playback"}))
    assert not (tmp_path / "serving.duckdb.generation").exists()
    serving.publish_if_changed(warehouse_file, str(target))
    assert (tmp_path / "serving.duckdb.generation").read_text() == "gen-1"


# --- long playback sessions -----------------------------------------------------

def _session(duration, extracted):
    return {
        "started_at": datetime.datetime(2026, 11, 9, 18, 53, 40, tzinfo=UTC),
        "user_id": "u1", "user_name": "maman", "item_id": "frank", "item_type": "Movie",
        "item_name": "Frankenstein", "series_name": None, "playback_method": "DirectPlay",
        "client_name": "Android TV", "device_name": "TV", "play_duration_seconds": duration,
    }, extracted


def test_a_long_session_gets_its_final_duration(tmp_path):
    """Frankenstein, 9 Nov 2026: last hourly read 7,912 s, real duration 9,170 s."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime.datetime(2026, 11, 9, 18, tzinfo=UTC)
    row, at = _session(7912, datetime.datetime(2026, 11, 9, 21, 5, tzinfo=UTC))
    assert _settle_playback_hour(lake, hour, [row], at)
    row, at = _session(9170, datetime.datetime(2026, 11, 10, 8, 20, tzinfo=UTC))
    assert _settle_playback_hour(lake, hour, [row], at)
    table = lake.read_table(f"{PLAYBACK_PREFIX}/partition_hour=2026-11-09T18:00:00/data.parquet")
    assert table.column("play_duration_seconds").to_pylist() == [9170]
    assert table.schema.names == _PLAYBACK_SCHEMA.names


def test_an_unchanged_hour_is_not_rewritten(tmp_path):
    """Rewriting it would make every analytics run rebuild the playback marts."""
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    hour = datetime.datetime(2026, 11, 9, 18, tzinfo=UTC)
    row, at = _session(9170, datetime.datetime(2026, 11, 10, 8, 20, tzinfo=UTC))
    assert _settle_playback_hour(lake, hour, [row], at)
    row, at = _session(9170, datetime.datetime(2026, 11, 10, 10, 20, tzinfo=UTC))
    assert not _settle_playback_hour(lake, hour, [row], at)


# --- memory ---------------------------------------------------------------------

def test_every_connection_is_bounded(tmp_path, monkeypatch):
    from homelab_datalake import duck

    monkeypatch.setattr(duck, "TEMP_DIRECTORY", str(tmp_path / "spill"))
    con = duck.connect()
    limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    temp = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    con.close()
    value, unit = limit.split()
    assert unit == "GiB" and float(value) < 3, limit
    assert temp == str(tmp_path / "spill")


def test_a_read_only_mount_still_connects(tmp_path, monkeypatch):
    from homelab_datalake import duck

    blocked = tmp_path / "ro"
    blocked.mkdir()
    blocked.chmod(0o500)
    monkeypatch.setattr(duck, "TEMP_DIRECTORY", str(blocked / "spill"))
    try:
        duck.connect().close()
    finally:
        blocked.chmod(0o700)
