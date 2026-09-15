"""The status page, host logs and Keycloak sources: parsing, and the rules that keep history."""

import gzip
import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from dagster import Definitions, build_schedule_context

from homelab_datalake import lakeio, layout
from homelab_datalake.assets import host_logs as host_logs_assets
from homelab_datalake.assets import keycloak, media_pipeline, prometheus, status_page
from homelab_datalake.parsing import host_logs
from homelab_datalake.resources import LakeStorage

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def lake(tmp_path):
    return LakeStorage(local_root=str(tmp_path / "lake"))


# ---------------------------------------------------------------------------- status page


def _volume(tmp_path, samples, observations, push=True):
    directory = tmp_path / "status-page"
    directory.mkdir(exist_ok=True)
    (directory / "metrics.json").write_text(json.dumps({"version": 1, "samples": samples}))
    (directory / "incidents.json").write_text(json.dumps({
        "version": 3,
        "services": {"srv-1": {"name": "Secondary", "hostname": "secondary.example.org", "lastSentinelStatus": "UP"}},
        "incidents": [{"id": "i1", "serverId": "srv-1", "status": "resolved", "startedAt": "2026-09-02T03:19:18Z"}],
        "transitions": [{"id": "t1", "serverId": "srv-1", "previousStatus": "DOWN", "sentinelStatus": "UP"}],
        "observations": observations,
        "pushSubscriptions": [{"endpoint": "https://push.example.org/CAPABILITY", "keys": {"auth": "AUTHKEY"}}] if push else [],
    }))
    return str(directory)


def _sample(at, cpu=10.0):
    return {"serverId": "srv-1", "serverName": "Secondary", "recordedAt": at, "cpuPercent": cpu,
            "ram": {"usedBytes": 1, "totalBytes": 2, "usagePercent": 50.0},
            "disks": [{"mountPoint": "/", "usedBytes": 3, "totalBytes": 4, "usagePercent": 75.0}]}


def test_status_page_samples_and_observations_land_by_day(lake, tmp_path):
    data = _volume(tmp_path,
                   [_sample("2026-08-08T00:04:23+00:00"), _sample("2026-08-09T00:04:23.500Z")],
                   [{"serverId": "srv-1", "observedAt": "2026-08-09T01:00:00.000Z", "sentinelStatus": "UP",
                     "source": "sentinel_poll"}])
    report = status_page.capture(lake, data, datetime(2026, 9, 15, tzinfo=UTC))
    assert report["metric_samples"]["partitions_written"] == 2
    day = lakeio.read_strict(lake, f"{status_page.SAMPLES_PREFIX}/dt=2026-08-08/data.parquet").to_pylist()
    assert day[0]["disks"][0]["mount_point"] == "/" and day[0]["cpu_percent"] == 10.0


def test_a_day_the_status_page_pruned_keeps_what_the_lake_has(lake, tmp_path):
    first = _volume(tmp_path, [_sample("2026-08-08T00:04:23Z"), _sample("2026-08-08T00:09:23Z")], [])
    status_page.capture(lake, first, datetime(2026, 9, 15, tzinfo=UTC))
    pruned = _volume(tmp_path, [_sample("2026-08-08T00:09:23Z")], [])
    report = status_page.capture(lake, pruned, datetime(2026, 11, 7, tzinfo=UTC))
    held = lakeio.read_strict(lake, f"{status_page.SAMPLES_PREFIX}/dt=2026-08-08/data.parquet")
    assert held.num_rows == 2 and report["metric_samples"]["partitions_written"] == 0


def test_push_subscriptions_never_reach_the_lake(lake, tmp_path):
    data = _volume(tmp_path, [_sample("2026-08-08T00:04:23Z")], [])
    status_page.capture(lake, data, datetime(2026, 9, 15, tzinfo=UTC))
    landed = "".join(str(lakeio.read_strict(lake, key).to_pylist())
                     for key in lakeio.list_keys(lake, "raw"))
    assert "CAPABILITY" not in landed and "AUTHKEY" not in landed
    assert "Secondary" in landed


# ---------------------------------------------------------------------------- host logs


def test_a_journal_entry_keeps_every_field():
    row = host_logs.journal_row({
        "__REALTIME_TIMESTAMP": "1789258621478427", "__CURSOR": "s=1;i=2", "_HOSTNAME": "primary",
        "MESSAGE": [104, 105, 255], "PRIORITY": "6", "SYSLOG_FACILITY": "10", "_PID": "841925",
        "SYSLOG_IDENTIFIER": "sshd", "_SYSTEMD_UNIT": "ssh.service", "CODE_FILE": "x.c",
    })
    assert row["dt"] == date(2026, 9, 13)  # 00:17:01 UTC, whatever the machine running the test
    assert row["message"].startswith("hi") and row["priority"] == 6 and row["facility"] == 10
    assert json.loads(row["fields_json"]) == {"CODE_FILE": "x.c"}
    pa.Table.from_pylist([row], schema=host_logs.JOURNAL_SCHEMA)


def test_auth_log_lines_get_their_year_from_the_file(tmp_path):
    current = tmp_path / "auth.log"
    current.write_text("Sep 13 00:17:01 primary CRON[841925]: pam_unix(cron:session): session opened\n"
                       "not a syslog line\n")
    rotated = tmp_path / "auth.log.2.gz"
    with gzip.open(rotated, "wt") as handle:
        handle.write("Dec 31 23:59:59 primary sshd[12]: Accepted publickey for operator\n")
    january = datetime(2027, 1, 2, tzinfo=UTC).timestamp()
    os.utime(rotated, (january, january))
    september = datetime(2026, 9, 15, tzinfo=UTC).timestamp()
    os.utime(current, (september, september))
    parsed = host_logs.read_auth_logs(str(tmp_path))
    assert parsed.unparsed == 1
    assert parsed.rows_by_day[date(2026, 9, 13)][0]["program"] == "CRON"
    december = parsed.rows_by_day[date(2026, 12, 31)][0]
    assert december["pid"] == 12 and december["source_file"] == "auth.log.2.gz"


# ---------------------------------------------------------------------------- keycloak


def test_the_grant_file_is_exactly_what_the_code_reads():
    assert (REPO / "infra/keycloak-datalake-grants.sql").read_text() == keycloak.render_grant_sql()


def test_no_keycloak_secret_is_ever_granted():
    granted = {column for columns in keycloak.GRANTED.values() for column in columns}
    assert not granted & {"secret", "registration_token", "value", "salt", "secret_data", "credential_data"}


def test_keycloak_events_merge_by_id_and_never_shrink(lake):
    key = f"{keycloak.EVENTS_PREFIX}/dt=2026-09-15/data.parquet"
    record = dict.fromkeys(keycloak.EVENT_COLUMNS)
    first = [{**record, "id": "a", "event_time": 1789430000000, "type": "LOGIN"},
             {**record, "id": "b", "event_time": 1789430001000, "type": "LOGOUT"}]
    keycloak.merge_events(lake, key, keycloak.event_rows(first, "event_time"), keycloak.EVENT_SCHEMA)
    before, after = keycloak.merge_events(lake, key, keycloak.event_rows(first[:1], "event_time"),
                                          keycloak.EVENT_SCHEMA)
    assert (before, after) == (2, 2)


def test_keycloak_reuses_the_litellm_role_unless_told_otherwise(monkeypatch):
    monkeypatch.setenv("LITELLM_PG_USER", "datalake_ro")
    monkeypatch.setenv("LITELLM_PG_HOST", "postgres")
    monkeypatch.delenv("KEYCLOAK_PG_USER", raising=False)
    settings = keycloak.connection_settings()
    assert settings["user"] == "datalake_ro" and settings["dbname"] == "keycloak"


def test_every_new_source_is_registered_where_the_catalogue_can_link_it():
    from homelab_datalake.api import catalogue

    for dataset in layout.RAW_DATASETS:
        assert catalogue.asset_path(dataset)[0].endswith("_raw"), dataset.table


# ---------------------------------------------------------------------------- schedules


@pytest.mark.parametrize("module", [host_logs_assets, keycloak, media_pipeline, prometheus, status_page],
                         ids=lambda m: m.__name__.rsplit(".", 1)[-1])
@pytest.mark.parametrize("hour", [0, 23])
def test_every_schedule_asks_only_for_partitions_that_exist(module, hour, tmp_path):
    # Dagster checks a requested partition when the tick runs, not when the code loads. A
    # schedule asking for a day that has not ended fails every hour in production and
    # passes everything else: keycloak_schedule asked for today with no end offset.
    defs = Definitions(assets=module.ASSETS, jobs=module.JOBS, schedules=module.SCHEDULES,
                       resources={"lake": LakeStorage(local_root=str(tmp_path))})
    repository = defs.get_repository_def()
    when = datetime(2026, 9, 16, hour, 59, tzinfo=UTC)
    for schedule in module.SCHEDULES:
        context = build_schedule_context(scheduled_execution_time=when, repository_def=repository)
        schedule.evaluate_tick(context)
