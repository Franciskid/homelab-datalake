"""Poll ingestion for politico.fraserv.fr: the latest voting-intention polls of every institute, read automatically.

Every morning the job looks for new reports (the Commission des sondages list, each institute's publication page, the French
Wikipedia table as a cross-check), downloads them, reads the tables with the parsers vendored from the politico repository
(parsing/polls/), validates every poll and writes `polls-export.json` into the directory shared with the politico stack,
next to `official-statistics.json`. The site fetches it at run time, so a new poll appears without a redeploy.

A poll is published ("auto") only when every check passes and its reader is one we measured against hand-checked records;
the others are listed for review, with the reasons, and never published. The export holds what is new compared to the polls
the site already bundles (read from https://politico.fraserv.fr/polls-index.json), and the previous export is the job's
memory: a report already read is not downloaded again.

Without the export directory (a test, a laptop) the job still runs and reports the export as skipped.
"""

import fcntl
import json
import os
import time
import tempfile
from urllib.error import HTTPError
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

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

from homelab_datalake.parsing.polls import ingest
from homelab_datalake.parsing.polls.fetch_pages import check_access

DEFAULT_EXPORT_DIR = "/exports/politico"
EXPORT_NAME = "polls-export.json"
INDEX_URL = "https://politico.fraserv.fr/polls-index.json"
SINCE_DAYS = 45
REQUEST_TIMEOUT_SECONDS = 60
ATTEMPTS = 2

CHECK_SOURCES = "sources_answered"
CHECK_EXPORTED = "export_written"
CHECK_REVIEW = "review_queue_short"
REVIEW_WARN_AT = 25


def http_fetch(url: str) -> tuple[bytes, str]:
    """GET over https only, a couple of tries, capped size. Raises on failure: the run reports that source as failed."""
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https source: {url!r}")
    last: Exception | None = None
    for attempt in range(ATTEMPTS):
        try:
            request = urllib.request.Request(  # noqa: S310
                url, headers={"User-Agent": ingest.USER_AGENT, "Accept": "text/html,application/pdf,application/json,*/*"})
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
                return response.read(ingest.MAX_PDF_BYTES + 1), response.headers.get("Content-Type", "")
        except HTTPError as error:
            check_access(b"", error.code)
            raise
        except OSError as error:
            last = error
            time.sleep(3 * (attempt + 1))
    raise OSError(f"{url} failed {ATTEMPTS} times") from last


def read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_export(directory: Path, export: dict, name: str = EXPORT_NAME) -> Path:
    """Write the file whole, then rename it over the old one: the site never reads half."""
    path = directory / name
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, prefix=f".{name}.", delete=False) as handle:
        tmp = Path(handle.name)
        try:
            json.dump(export, handle, ensure_ascii=False, separators=(",", ":"))
            handle.close()
            tmp.chmod(0o644)
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)
    return path


def load_baseline(fetch, seed: Path | None, log) -> dict | None:
    """What the site already has. The live index is current; the seed (if any) is only for a site that cannot be reached."""
    try:
        data, _ = fetch(os.environ.get("POLLS_INDEX_URL", INDEX_URL))
        index = json.loads(data)
        if index.get("candidates") and len(index.get("rounds", [])) == 2:
            return index
    except Exception as error:  # noqa: BLE001
        log.warning("polls index not readable from the site: %s", error)
    return read_json(seed) if seed else None


def run_job(fetch, directory: Path | None, today: datetime, log) -> dict:
    previous = read_json(directory / EXPORT_NAME) if directory else None
    seed = directory / "polls-index.json" if directory else None
    baseline = load_baseline(fetch, seed, log)
    if baseline is None:
        raise RuntimeError("no baseline of the site's polls (polls-index.json): refusing to ingest blind, duplicates would follow")
    export = ingest.run(fetch, baseline, previous, today.date(), since_days=SINCE_DAYS, now=today, log=log.info)
    return export


@asset(
    group_name="polls",
    kinds={"python", "api"},
    retry_policy=RetryPolicy(max_retries=1, delay=600),
    check_specs=[
        AssetCheckSpec(name=CHECK_SOURCES, asset="polls_ingestion",
                       description="Warns when a source (Commission list, an institute page, Wikipedia) could not be read today."),
        AssetCheckSpec(name=CHECK_EXPORTED, asset="polls_ingestion",
                       description="Warns when the export for politico.fraserv.fr was not written."),
        AssetCheckSpec(name=CHECK_REVIEW, asset="polls_ingestion",
                       description=f"Warns when more than {REVIEW_WARN_AT} reports wait for a human review."),
    ],
    description=(
        "The latest voting-intention polls of every institute: discovered, downloaded, read, validated, and exported for "
        "politico.fraserv.fr. Auto-published polls are marked unreviewed; the rest wait in the review list."
    ),
)
def polls_ingestion(context: AssetExecutionContext) -> MaterializeResult:
    now = datetime.now(UTC)
    directory = Path(os.environ.get("POLLS_EXPORT_DIR", os.environ.get("OFFICIAL_STATS_EXPORT_DIR", DEFAULT_EXPORT_DIR)))
    if not directory.is_dir():
        raise RuntimeError(f"poll export directory {directory} is not mounted")
    with (directory / ".polls.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        export = run_job(http_fetch, directory, now, context.log)
        exported = str(write_export(directory, export))
        queue = str(write_export(directory, {"generated_at": export["generated_at"], "review": export["review"],
                                            "pending_notices": export["pending_notices"]}, "polls-review.json"))
    job = export["job"]
    sources_failed = [s["source"] for s in job["sources"] if not s.get("ok")]
    context.log.info("polls: %d reports found, %d ingested, %d to review, %d duplicates, sources failed: %s, Wikipedia-only: %d",
                     job["discovered"], job["ingested"], job["review"], job["duplicates"], sources_failed or "none", len(job["wikipedia_only"]))
    review = len(export["review"])
    context.log.info("polls: %d total waiting for review in %s, Firecrawl cloud estimated credits: %d",
                     review, queue, job["firecrawl_cloud_credits"])
    for source in job["sources"]:
        if source.get("error"):
            context.log.warning("poll source %s: %s", source["source"], source["error"])
    if job["monitoring"]:
        raise RuntimeError("poll monitoring failed: " + "; ".join(job["monitoring"]))
    return MaterializeResult(
        metadata={"discovered": job["discovered"], "downloaded": job["downloaded"], "ingested": job["ingested"], "review": review,
                  "duplicates": job["duplicates"], "published_polls": sum(len(r["polls"]) for r in export["rounds"]),
                  "sources_failed": ", ".join(sources_failed) or "none", "wikipedia_only": len(job["wikipedia_only"]),
                  "export": exported or "skipped"},
        check_results=[
            AssetCheckResult(check_name=CHECK_SOURCES, passed=not sources_failed, severity=AssetCheckSeverity.WARN,
                             metadata={"failed": ", ".join(sources_failed) or "none"}),
            AssetCheckResult(check_name=CHECK_EXPORTED, passed=exported is not None, severity=AssetCheckSeverity.WARN,
                             metadata={"path": exported or str(directory)}),
            AssetCheckResult(check_name=CHECK_REVIEW, passed=review <= REVIEW_WARN_AT, severity=AssetCheckSeverity.WARN,
                             metadata={"waiting": review}),
        ],
    )


polls_ingestion_job = define_asset_job(
    name="polls_ingestion_daily", selection=[polls_ingestion],
    description="Find new voting-intention polls, read and validate them, export them for politico.fraserv.fr.",
)


@schedule(job=polls_ingestion_job, cron_schedule="10 6 * * *", default_status=DefaultScheduleStatus.RUNNING,
          description=("Daily at 06:10 UTC, after the official statistics (05:40). The Commission des sondages publishes a notice a "
                       "day or two after a poll; the institutes' own pages are usually earlier, so a morning read is never more than a day late."))
def polls_ingestion_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"polls-ingestion-{context.scheduled_execution_time:%Y%m%d}")


ASSETS = [polls_ingestion]
JOBS = [polls_ingestion_job]
SCHEDULES = [polls_ingestion_schedule]
