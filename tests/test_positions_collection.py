"""Positions collection: the jobs are wired (names, schedules), a failing step never stops the others, and the vendored scripts run."""

import json
import logging
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from dagster import Failure

from homelab_datalake.assets import positions_collection as pc

log = logging.getLogger("test")


class Proc:
    def __init__(self, code=0, out="", err=""):
        self.returncode, self.stdout, self.stderr = code, out, err


def test_job_names_never_clash_with_asset_names():
    """A job named like an asset stopped the code location from loading once."""
    assets = {a.key.to_user_string() for a in pc.ASSETS}
    jobs = {j.name for j in pc.JOBS}
    assert not assets & jobs
    assert {"positions_watch_daily", "positions_votes_daily", "positions_texts_weekly", "positions_backfill_manual"} == jobs


@pytest.mark.skipif(shutil.which("dbt") is None, reason="the code location builds a dbt resource: needs the virtualenv's dbt on PATH")
def test_the_code_location_loads_with_the_new_definitions():
    from homelab_datalake.definitions import defs

    repo = defs.get_repository_def()
    for name in ("positions_watch_daily", "positions_votes_daily", "positions_texts_weekly", "positions_backfill_manual"):
        assert repo.has_job(name)
    assert {s.name for s in pc.SCHEDULES} <= {s.name for s in repo.schedule_defs}


def test_schedules_are_staggered_and_after_the_polls():
    crons = {s.name: s.cron_schedule for s in pc.SCHEDULES}
    assert crons["positions_watch_schedule"] == "40 6 * * *"          # after the poll ingestion (06:10)
    assert crons["positions_votes_schedule"] == "10 5 * * *"
    assert crons["positions_texts_schedule"] == "30 7 * * 1"          # weekly: the debate archives are heavy


def test_a_failing_step_does_not_stop_the_next_ones(tmp_path):
    calls = []

    def runner(args, root, timeout=0):
        step = args[1]
        calls.append(step)
        if step == "votes-senat":
            return Proc(1, "", "Traceback ...")
        return Proc(0, json.dumps({step: {"ballots": 3}, "_downloaded": [{"url": "u", "bytes": 5, "sha256": "x"}]}))

    runs = pc.collect_steps("votes", tmp_path, log, runner)
    assert calls == ["votes-an", "votes-senat", "votes-ep"]
    assert [r["ok"] for r in runs] == [True, False, True]
    assert runs[0]["downloaded"] == [{"url": "u", "bytes": 5}]
    assert "Traceback" in runs[1]["stderr_tail"]


def test_a_collector_that_reports_its_own_error_counts_as_failed(tmp_path):
    runner = lambda args, root, timeout=0: Proc(0, json.dumps({args[1]: {"error": "CircuitOpen: data.senat.fr"}}))  # noqa: E731
    runs = pc.collect_steps("votes", tmp_path, log, runner)
    assert not any(r["ok"] for r in runs)
    assert "CircuitOpen" in runs[0]["summary"]


def test_steps_always_follow_the_tracked_people_with_revalidation(tmp_path):
    seen = []
    pc.collect_steps("texts", tmp_path, log, lambda args, root, timeout=0: seen.append(args) or Proc(0, "{}"))
    for args in seen:
        assert args[args.index("--people") + 1] == "tracked" and "--revalidate" in args
        assert Path(args[args.index("--db") + 1]).parts[-3:] == ("research", "positions", "positions.sqlite")
    assert "--limit" in seen[0]   # speeches are capped per run (3 s between pages)


def test_cycle_collects_then_exports(tmp_path):
    out = tmp_path / "export"
    out.mkdir()
    seen = []

    def runner(args, root, timeout=0):
        seen.append(Path(args[0]).name)
        if Path(args[0]).name == "export_delta.py":
            return Proc(0, json.dumps({"seq": 4, "new": True, "delta": {"counts": {"documents": 2, "ballots": 1, "votes": 7}}}))
        return Proc(0, json.dumps({args[1]: {"ok": True}}))

    result = pc.run_cycle("votes", log, runner, root=tmp_path / "state", out=out)
    assert seen == ["collect.py"] * 3 + ["export_delta.py"]
    assert result["exported"] and result["export"]["seq"] == 4 and not result["failed"]


def test_watch_cycle_runs_the_watcher_then_the_detector_and_survives_a_failing_watcher(tmp_path, monkeypatch):
    monkeypatch.setenv("POLLS_EXPORT_DIR", str(tmp_path / "polls"))
    monkeypatch.setattr(pc, "fetch_polls_index", lambda root, log: None)
    seen = []

    def runner(args, root, timeout=0):
        seen.append(Path(args[0]).name)
        return Proc(1, "", "boom") if Path(args[0]).name == "watch_programmes.py" else Proc(0, "{}")

    runs = pc.watch_steps(tmp_path, tmp_path / "out", log, runner)
    assert seen == ["watch_programmes.py", "candidacies.py"]
    assert [r["ok"] for r in runs] == [False, True]


def test_every_step_failing_fails_the_run(tmp_path, monkeypatch):
    out = tmp_path / "export"
    out.mkdir()
    monkeypatch.setattr(pc, "export_dir", lambda: out)
    monkeypatch.setattr(pc, "state_root", lambda: tmp_path / "state")
    monkeypatch.setattr(pc, "run_script", lambda args, root, timeout=0: Proc(1, "", "down"))

    class Ctx:
        log = log

    with pytest.raises(Failure):
        pc.materialize("votes", Ctx())


def test_vendored_scripts_run_offline_and_export_a_delta(tmp_path):
    """The vendored collector builds its database from the frozen roster (no poll file here), and the exporter reads it."""
    root, out = tmp_path / "state", tmp_path / "export"
    env = pc.script_env(root)
    people = subprocess.run([sys.executable, "-I", str(pc.VENDORED / "collect.py"), "people", "--db", str(pc.db_path(root))],
                            env=env, capture_output=True, text=True, check=False)
    assert people.returncode == 0, people.stderr
    assert "marine-le-pen" in people.stdout and "orbit" in people.stdout
    exported = subprocess.run([sys.executable, "-I", str(pc.VENDORED / "export_delta.py"), "--db", str(pc.db_path(root)), "--out", str(out)],
                              env=env, capture_output=True, text=True, check=False)
    assert exported.returncode == 0, exported.stderr
    assert json.loads(exported.stdout)["new"] is False   # no document and no vote yet
    assert (out / "manifest.json").exists()
