"""The Insee's Fichier des prénoms for politico.fraserv.fr's Données: found, kept in the lake, cut into small files, exported.

Published once a year (July). The weekly job looks at the Insee's series page, finds the newest edition, and does nothing when the
export already holds it. A new edition: the two zips (national, by département) are kept in the lake as inputs (manual_inputs.put with
how "fetched: plain request", the same manifest the hand-fed inputs use), cut into per-letter files (parsing/first_names.py) and
written whole to `<export dir>/prenoms/`, swapped in with a rename so the renderer never reads half of them.
"""

import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
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

from homelab_datalake import manual_inputs
from homelab_datalake.assets.official_statistics import DEFAULT_EXPORT_DIR, _get
from homelab_datalake.parsing import first_names as fn
from homelab_datalake.resources import LakeStorage

SOURCE = "insee_prenoms"
EXPORT_SUBDIR = "prenoms"


def find_edition(get_text=lambda url: _get(url).decode("utf-8")) -> dict:
    """The newest edition's links, from the series page (the page number of an edition changes every year)."""
    series = get_text(fn.SERIES_PAGE)
    best = None
    for page in fn.candidate_pages(series):
        links = fn.edition_links(get_text(page))
        if links and (best is None or links["edition"] > best["edition"]):
            best = {**links, "page": page}
    if best is None:
        raise RuntimeError(f"no Fichier des prénoms found from {fn.SERIES_PAGE}: the page has changed")
    return best


def exported_edition(directory: Path) -> tuple[int, int] | None:
    """(edition, format of the files) of what the export holds, or None."""
    try:
        m = json.loads((directory / EXPORT_SUBDIR / "manifest.json").read_text(encoding="utf-8"))
        return m.get("edition"), m.get("format")
    except (OSError, ValueError):
        return None


def write_directory(directory: Path, files: dict[str, dict]) -> Path:
    """Every file written, then the directory renamed over the old one."""
    target = directory / EXPORT_SUBDIR
    tmp = directory / f".{EXPORT_SUBDIR}.tmp-{os.getpid()}"
    old = directory / f".{EXPORT_SUBDIR}.old"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    for name, content in files.items():
        (tmp / name).write_text(json.dumps(content, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    shutil.rmtree(old, ignore_errors=True)
    if target.exists():
        target.rename(old)
    tmp.rename(target)
    shutil.rmtree(old, ignore_errors=True)
    return target


def run(directory: Path, lake: LakeStorage | None, log, *, find=find_edition, get=_get, force: bool = False) -> dict:
    found = find()
    if not force and exported_edition(directory) == (found["edition"], fn.FORMAT):
        log.info("first names: edition %s already exported", found["edition"])
        return {"edition": found["edition"], "skipped": True}
    nat_zip, dpt_zip = get(found["nat"]), get(found["dpt"])
    if lake is not None:
        for role, data, url in (("nat_zip", nat_zip, found["nat"]), ("dpt_zip", dpt_zip, found["dpt"])):
            manual_inputs.put(lake, SOURCE, role, url.rsplit("/", 1)[-1], data, edition=found["edition"], url=url, how="fetched: plain request",
                              note="Insee, Fichier des prénoms (Licence Ouverte); counts rounded to 5, rare names not published")
    nat = fn.read_nat(fn.csv_in_zip(nat_zip))
    dpt = fn.read_dpt(fn.csv_in_zip(dpt_zip))
    files = fn.build(nat, dpt, found["edition"], {"page": found["page"], "nat": found["nat"], "dpt": found["dpt"],
                                                  "built_on": datetime.now(UTC).date().isoformat()})
    target = write_directory(directory, files)
    log.info("first names: edition %s, %d files in %s", found["edition"], len(files), target)
    return {"edition": found["edition"], "files": len(files), "names": files["manifest.json"]["names"], "skipped": False}


@asset(
    group_name="official_statistics", kinds={"python", "api"}, retry_policy=RetryPolicy(max_retries=2, delay=600),
    description="Insee's Fichier des prénoms, cut into per-letter files for politico.fraserv.fr's Données (the page of first names).",
)
def first_names_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    directory = Path(os.environ.get("OFFICIAL_STATS_EXPORT_DIR", DEFAULT_EXPORT_DIR))
    if not directory.is_dir():
        context.log.warning("export directory %s is not mounted; nothing written", directory)
        return MaterializeResult(metadata={"export": "skipped"})
    result = run(directory, lake, context.log)
    return MaterializeResult(metadata={k: str(v) for k, v in result.items()})


first_names_job = define_asset_job(name="first_names", selection=[first_names_raw],
                                   description="Look for a new Fichier des prénoms; cut and export it when there is one.")


@schedule(job=first_names_job, cron_schedule="50 5 * * 2", default_status=DefaultScheduleStatus.RUNNING,
          description="Weekly, Tuesday 05:50 UTC: the file is published once a year in July; the run does nothing when the export holds the newest edition.")
def first_names_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"first-names-{context.scheduled_execution_time:%Y%W}")


ASSETS = [first_names_raw]
JOBS = [first_names_job]
SCHEDULES = [first_names_schedule]
