"""Automatic collection of the 2027 candidates' statements and programmes, for politico.fraserv.fr's positions pipeline.

Three scheduled jobs run the politico repository's collectors (vendored in parsing/positions/, run as subprocesses so their generic
module names stay out of the Dagster process) against a database that lives in the `positions-state` volume, then write what is
new as a small delta into the export directory the Mac pulls (rsync over ssh):

  positions_watch_daily   06:40 UTC  the 2027 programme watcher (each candidate's official page: fetch, hash, store a new version),
                                     then the candidacy detector (new candidacies / withdrawals: polls, watcher, Wikipedia)
  positions_votes_daily   05:10 UTC  Assemblée, Sénat and European Parliament (HowTheyVote, 9th and 10th terms) ballots for the
                                     candidates and their orbit
  positions_texts_weekly  Mon 07:30  new vie-publique speeches (capped), Assemblée and Sénat debates (comptes rendus intégraux)

Polite and resumable: robots.txt (RFC 9309), a rate limit per host, a circuit breaker after 8 consecutive 5xx answers, every download
cached with its sidecar (a big dump is fetched again only when the server's ETag / Last-Modified changed), every document stored once.
A first full collection is the manual job `positions_backfill_manual`. Nothing here publishes anything: the export is data for the Mac,
which merges it and queues the model (scripts/positions/sync.py in the politico repository).

Environment: POSITIONS_STATE_DIR (default /state/positions), POSITIONS_EXPORT_DIR (default /exports/positions),
POLLS_EXPORT_DIR (default /exports/politico, where polls-export.json is), POLLS_INDEX_URL.
Without the export directory (a test, a laptop) the job still collects and reports the export as skipped.
"""

import fcntl
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DefaultScheduleStatus,
    Failure,
    MaterializeResult,
    RetryPolicy,
    RunRequest,
    ScheduleEvaluationContext,
    asset,
    define_asset_job,
    schedule,
)

VENDORED = Path(__file__).resolve().parent.parent / "parsing" / "positions"
DEFAULT_STATE_DIR = "/state/positions"
DEFAULT_EXPORT_DIR = "/exports/positions"
DEFAULT_POLLS_DIR = "/exports/politico"
POLLS_INDEX_URL = "https://politico.fraserv.fr/polls-index.json"
STEP_TIMEOUT_SECONDS = 4 * 3600

CHECK_STEPS = "steps_succeeded"
CHECK_EXPORTED = "export_written"

# step -> extra arguments. People are always `tracked` (the candidates and their orbit).
CYCLES: dict[str, list[tuple[str, list[str]]]] = {
    "votes": [("votes-an", ["--legislatures", "17"]), ("votes-senat", []), ("votes-ep", [])],
    "texts": [("speeches", ["--limit", "120", "--per-person", "15"]), ("debats-an", []), ("debats-senat", [])],
    "backfill": [("votes-an", ["--legislatures", "16,17"]), ("votes-senat", []), ("votes-ep", []),
                 ("speeches", ["--per-person", "25"]), ("debats-an", []), ("debats-senat", [])],
}


def state_root() -> Path:
    return Path(os.environ.get("POSITIONS_STATE_DIR", DEFAULT_STATE_DIR))


def export_dir() -> Path:
    return Path(os.environ.get("POSITIONS_EXPORT_DIR", DEFAULT_EXPORT_DIR))


def db_path(root: Path) -> Path:
    return root / "research" / "positions" / "positions.sqlite"


def script_env(root: Path) -> dict:
    return {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "/tmp"), "POSITIONS_ROOT": str(root),
            "SSL_CERT_FILE": os.environ.get("SSL_CERT_FILE", ""), "LANG": "C.UTF-8"}


def run_script(args: list[str], root: Path, timeout: int = STEP_TIMEOUT_SECONDS) -> subprocess.CompletedProcess:
    """One vendored script, in isolated mode (`-I`), with its own root. Never raises on a non-zero exit."""
    return subprocess.run([sys.executable, "-I", *args], env=script_env(root), capture_output=True, text=True,  # noqa: S603
                          timeout=timeout, check=False)


@contextmanager
def state_lock(root: Path):
    """One collection at a time on the shared database and download cache (the three jobs are staggered; this is the guarantee)."""
    root.mkdir(parents=True, exist_ok=True)
    with open(root / ".collect.lock", "a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def summarise(step: str, result: dict | None) -> str:
    """One line for the logs / the manifest: counts only, whatever the collector returned."""
    if not isinstance(result, dict):
        return "no result"
    if "error" in result:
        return f"error: {result['error']}"[:200]
    return json.dumps({k: v for k, v in result.items() if not isinstance(v, (dict, list))}, ensure_ascii=False)[:200] or "ok"


def collect_steps(kind: str, root: Path, log, runner=None) -> list[dict]:
    runner = runner or run_script
    runs = []
    for step, extra in CYCLES[kind]:
        started = time.monotonic()
        entry = {"step": step, "ok": False}
        try:
            proc = runner([str(VENDORED / "collect.py"), step, "--people", "tracked", "--db", str(db_path(root)), "--revalidate", *extra], root)
            entry["code"] = proc.returncode
            payload = None
            try:
                payload = json.loads(proc.stdout) if proc.stdout.strip() else None
            except ValueError:
                payload = None
            result = (payload or {}).get(step)
            entry["summary"] = summarise(step, result)
            entry["downloaded"] = [{"url": d["url"], "bytes": d["bytes"]} for d in (payload or {}).get("_downloaded", [])]
            entry["ok"] = proc.returncode == 0 and isinstance(result, dict) and "error" not in result
            if not entry["ok"]:
                entry["stderr_tail"] = proc.stderr.strip()[-400:]
        except subprocess.TimeoutExpired:
            entry["summary"] = f"timed out after {STEP_TIMEOUT_SECONDS}s"
        entry["seconds"] = round(time.monotonic() - started)
        log.info("positions %s: %s (%ss) %s", step, "ok" if entry["ok"] else "FAILED", entry["seconds"], entry.get("summary", ""))
        runs.append(entry)
    return runs


def fetch_polls_index(root: Path, log) -> Path | None:
    """The site's current polls index (the candidate ids it knows), best effort: a failure only means one signal fewer."""
    target = root / "polls-index.json"
    try:
        request = urllib.request.Request(os.environ.get("POLLS_INDEX_URL", POLLS_INDEX_URL),  # noqa: S310
                                         headers={"User-Agent": "Mozilla/5.0 (compatible; observatoire-positions/0.1; non-commercial research crawler)"})
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            data = response.read(5_000_000)
        json.loads(data)
        target.write_bytes(data)
        return target
    except Exception as error:  # noqa: BLE001
        log.warning("polls index not readable: %s", error)
        return target if target.exists() else None


def watch_steps(root: Path, out: Path | None, log, runner=None) -> list[dict]:
    runner = runner or run_script
    runs = []
    started = time.monotonic()
    proc = runner([str(VENDORED / "watch_programmes.py"), "--people", "main", "--db", str(db_path(root))], root)
    runs.append({"step": "programme-watch", "ok": proc.returncode == 0, "code": proc.returncode, "seconds": round(time.monotonic() - started),
                 "summary": proc.stderr.strip().splitlines()[-1][:200] if proc.stderr.strip() else "",
                 **({} if proc.returncode == 0 else {"stderr_tail": proc.stderr.strip()[-400:]})})
    log.info("positions programme-watch: %s", "ok" if proc.returncode == 0 else "FAILED")
    polls_dir = Path(os.environ.get("POLLS_EXPORT_DIR", DEFAULT_POLLS_DIR))
    polls = [p for p in (polls_dir / "polls-export.json", fetch_polls_index(root, log)) if p and p.exists()]
    watch_file = root / "research" / "positions" / "programme-watch.json"
    started = time.monotonic()
    args = [str(VENDORED / "candidacies.py"), "detect", "--out", str(out or root / "proposals"), "--polls", ",".join(map(str, polls))]
    if watch_file.exists():
        args += ["--watch", str(watch_file)]
    proc = runner(args, root)
    runs.append({"step": "candidacy-detector", "ok": proc.returncode == 0, "code": proc.returncode, "seconds": round(time.monotonic() - started),
                 "summary": proc.stdout.strip().replace("\n", " ")[:200], **({} if proc.returncode == 0 else {"stderr_tail": proc.stderr.strip()[-400:]})})
    log.info("positions candidacy-detector: %s %s", "ok" if proc.returncode == 0 else "FAILED", runs[-1]["summary"])
    return runs


def export_cycle(root: Path, out: Path | None, runs: list[dict], log, runner=None, since: str | None = None) -> dict | None:
    """Write the delta (what is new since the last export) into the export directory. None when there is no export directory."""
    runner = runner or run_script
    if out is None:
        return None
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump({"cycle": runs, "at": datetime.now(UTC).isoformat()}, handle)
    try:
        args = [str(VENDORED / "export_delta.py"), "--db", str(db_path(root)), "--out", str(out), "--runs", handle.name]
        proc = runner([*args, *(["--since", since] if since else [])], root, 3600)
    finally:
        os.unlink(handle.name)
    if proc.returncode != 0:
        log.error("positions export failed: %s", proc.stderr.strip()[-400:])
        return None
    watch_file = root / "research" / "positions" / "programme-watch.json"
    if watch_file.exists():
        tmp = out / ".programme-watch.json.tmp"
        tmp.write_bytes(watch_file.read_bytes())
        os.replace(tmp, out / "programme-watch.json")
    return json.loads(proc.stdout)


def run_cycle(kind: str, log, runner=None, root: Path | None = None, out: Path | None = None, since: str | None = None) -> dict:
    root = root or state_root()
    if out is None:
        candidate = export_dir()
        out = candidate if candidate.is_dir() else None
    if out is None:
        log.warning("export directory %s is not mounted; export skipped", export_dir())
    with state_lock(root):
        runs = watch_steps(root, out, log, runner) if kind == "watch" else collect_steps(kind, root, log, runner)
        exported = export_cycle(root, out, runs, log, runner, since)
    return {"kind": kind, "runs": runs, "failed": [r["step"] for r in runs if not r["ok"]], "export": exported, "exported": out is not None and exported is not None}


def materialize(kind: str, context: AssetExecutionContext) -> MaterializeResult:
    result = run_cycle(kind, context.log)
    failed = result["failed"]
    if failed and len(failed) == len(result["runs"]):
        raise Failure(description=f"every {kind} step failed: " + "; ".join(f"{r['step']}: {r.get('summary', '')}" for r in result["runs"]))
    delta = (result["export"] or {}).get("delta") or {}
    counts = delta.get("counts", {})
    return MaterializeResult(
        metadata={"steps": ", ".join(r["step"] for r in result["runs"]), "failed": ", ".join(failed) or "none",
                  "new_documents": counts.get("documents", 0), "new_ballots": counts.get("ballots", 0), "new_votes": counts.get("votes", 0),
                  "delta_seq": (result["export"] or {}).get("seq", 0), "export": str(export_dir()) if result["exported"] else "skipped"},
        check_results=[
            AssetCheckResult(check_name=CHECK_STEPS, passed=not failed, severity=AssetCheckSeverity.WARN, metadata={"failed": ", ".join(failed) or "none"}),
            AssetCheckResult(check_name=CHECK_EXPORTED, passed=result["exported"], severity=AssetCheckSeverity.WARN, metadata={"dir": str(export_dir())}),
        ],
    )


def _checks(asset_name: str) -> list[AssetCheckSpec]:
    return [AssetCheckSpec(name=CHECK_STEPS, asset=asset_name, description="Warns when a collection step failed or timed out today."),
            AssetCheckSpec(name=CHECK_EXPORTED, asset=asset_name, description="Warns when the delta for the Mac was not written.")]


@asset(name="positions_programme_watch", group_name="positions", kinds={"python", "api"}, retry_policy=RetryPolicy(max_retries=1, delay=900),
       check_specs=_checks("positions_programme_watch"),
       description="The candidates' official programme pages: fetched, hashed, a new version stored when one changes; then new candidacies and withdrawals proposed.")
def positions_programme_watch(context: AssetExecutionContext) -> MaterializeResult:
    return materialize("watch", context)


@asset(name="positions_votes", group_name="positions", kinds={"python", "api"}, retry_policy=RetryPolicy(max_retries=1, delay=900),
       check_specs=_checks("positions_votes"),
       description="Assemblée nationale, Sénat and European Parliament (9th and 10th terms) ballots of the candidates and their orbit.")
def positions_votes(context: AssetExecutionContext) -> MaterializeResult:
    return materialize("votes", context)


@asset(name="positions_texts", group_name="positions", kinds={"python", "api"}, retry_policy=RetryPolicy(max_retries=1, delay=1800),
       check_specs=_checks("positions_texts"),
       description="New vie-publique speeches and the Assemblée / Sénat debates of the candidates and their orbit.")
def positions_texts(context: AssetExecutionContext) -> MaterializeResult:
    return materialize("texts", context)


@asset(name="positions_backfill", group_name="positions", kinds={"python", "api"}, check_specs=_checks("positions_backfill"),
       description="First full collection (manual): every step with its history. Hours long; resumable, run it again after an interruption.")
def positions_backfill(context: AssetExecutionContext) -> MaterializeResult:
    return materialize("backfill", context)


# Job names must differ from the asset names: a clash stopped the code location from loading once (see polls_ingestion).
positions_watch_job = define_asset_job(name="positions_watch_daily", selection=[positions_programme_watch],
                                       description="Programme watcher and candidacy detector, then the export for the Mac.")
positions_votes_job = define_asset_job(name="positions_votes_daily", selection=[positions_votes],
                                       description="Ballots of the candidates and their orbit, then the export for the Mac.")
positions_texts_job = define_asset_job(name="positions_texts_weekly", selection=[positions_texts],
                                       description="New speeches and debates of the candidates and their orbit, then the export for the Mac.")
positions_backfill_job = define_asset_job(name="positions_backfill_manual", selection=[positions_backfill],
                                          description="First full collection. Run by hand.")


@schedule(job=positions_watch_job, cron_schedule="40 6 * * *", default_status=DefaultScheduleStatus.RUNNING,
          description="Daily at 06:40 UTC, after the poll ingestion (06:10): the candidacy detector reads the polls it just ingested.")
def positions_watch_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"positions-watch-{context.scheduled_execution_time:%Y%m%d}")


@schedule(job=positions_votes_job, cron_schedule="10 5 * * *", default_status=DefaultScheduleStatus.RUNNING,
          description="Daily at 05:10 UTC: the Sénat's dumps are rebuilt before 05:00, the Assemblée's overnight.")
def positions_votes_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"positions-votes-{context.scheduled_execution_time:%Y%m%d}")


@schedule(job=positions_texts_job, cron_schedule="30 7 * * 1", default_status=DefaultScheduleStatus.RUNNING,
          description="Mondays at 07:30 UTC: the debate archives are several hundred MB each, so once a week; the speeches dump is checked at the same time.")
def positions_texts_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"positions-texts-{context.scheduled_execution_time:%G%V}")


ASSETS = [positions_programme_watch, positions_votes, positions_texts, positions_backfill]
JOBS = [positions_watch_job, positions_votes_job, positions_texts_job, positions_backfill_job]
SCHEDULES = [positions_watch_schedule, positions_votes_schedule, positions_texts_schedule]
