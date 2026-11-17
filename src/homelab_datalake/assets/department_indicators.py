"""Insee indicators by département for politico.fraserv.fr's « Croiser deux chiffres »: fetched weekly, kept in the lake, exported as `departements.json`.

The zips are large (25 MB each) and change a few times a year, so the job runs weekly and, like first_names.py, writes the export whole.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from dagster import AssetExecutionContext, DefaultScheduleStatus, MaterializeResult, RetryPolicy, RunRequest, ScheduleEvaluationContext, asset, define_asset_job, schedule

from homelab_datalake import manual_inputs
from homelab_datalake.assets.official_statistics import DEFAULT_EXPORT_DIR, _get
from homelab_datalake.parsing import department_indicators as di
from homelab_datalake.resources import LakeStorage

SOURCE = "insee_departements"
EXPORT_NAME = "departements.json"
MIN_DEPS = 90


def build(get=_get, lake: LakeStorage | None = None, log=None) -> dict:
    indicators: dict[str, dict] = {}
    sources = {}
    for key, src in di.SOURCES.items():
        data = get(src["zip"])
        if lake is not None:
            manual_inputs.put(lake, SOURCE, f"{key}_zip", src["zip"].rsplit("/", 1)[-1], data, edition=2023, url=src["zip"], how="fetched: plain request",
                              note="Insee (Licence Ouverte)")
        sources[key] = {k: src[k] for k in ("label", "title", "page")}
        for name, ind in di.PARSERS[key](data).items():
            if len(ind["values"]) < MIN_DEPS:
                raise ValueError(f"{name}: {len(ind['values'])} départements only")
            indicators[name] = {**ind, "source": key}
        if log:
            log.info("department indicators: %s read", key)
    return {"generated_at": datetime.now(UTC).isoformat(timespec="seconds"), "sources": sources, "indicators": indicators}


def write(directory: Path, doc: dict) -> Path:
    path = directory / EXPORT_NAME
    tmp = directory / f".{EXPORT_NAME}.tmp"
    tmp.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
    return path


@asset(group_name="official_statistics", kinds={"python", "api"}, retry_policy=RetryPolicy(max_retries=2, delay=600),
       description="Insee indicators by département (Filosofi 2023, recensement 2023) for politico.fraserv.fr's « Croiser deux chiffres ».")
def department_indicators_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    directory = Path(os.environ.get("OFFICIAL_STATS_EXPORT_DIR", DEFAULT_EXPORT_DIR))
    if not directory.is_dir():
        context.log.warning("export directory %s is not mounted; nothing written", directory)
        return MaterializeResult(metadata={"export": "skipped"})
    doc = build(lake=lake, log=context.log)
    path = write(directory, doc)
    return MaterializeResult(metadata={"indicators": len(doc["indicators"]), "export": str(path)})


department_indicators_job = define_asset_job(name="department_indicators", selection=[department_indicators_raw],
                                             description="Fetch the Insee indicators by département and export them.")


@schedule(job=department_indicators_job, cron_schedule="30 5 * * 3", default_status=DefaultScheduleStatus.RUNNING,
          description="Weekly, Wednesday 05:30 UTC (the sources change a few times a year).")
def department_indicators_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"department-indicators-{context.scheduled_execution_time:%Y%W}")


ASSETS = [department_indicators_raw]
JOBS = [department_indicators_job]
SCHEDULES = [department_indicators_schedule]
