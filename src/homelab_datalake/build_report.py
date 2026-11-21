"""What went wrong in one analytics run, shared between its steps.

analytics_hourly no longer stops at the first failure. A raw table that will not
load, a dbt model that errors or a dbt test that fails now costs only its own lineage:
everything else is built and published. Until 21 Nov 2026 it cost everything. Seven
official-statistics sources nobody had added to an accepted_values list failed one dbt
test, the dbt step failed, the publish was skipped, and every dataset the lake serves
(web traffic, LLM spend, cameras, Jellyfin) stayed frozen at 14 Nov.

The run must still end red when something failed, or nobody hears about it. So each
step records its failures here, the publish reads which marts to hold back, and the
last step of the run (`analytics_build_health`) fails the run with the whole list
once the healthy part has been published.

The steps run in separate processes, so this is a file beside the warehouse, keyed by
run id: a report left by another run reads as empty.
"""

import json
import os
from pathlib import Path
from typing import Any

REPORT_PATH = Path(
    os.environ.get("DUCKDB_PATH", "warehouse/datalake.duckdb")
).with_name("analytics_build_report.json")


def _load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def read(run_id: str, path: Path = REPORT_PATH) -> dict[str, Any]:
    report = _load(path)
    if report.get("run_id") != run_id:
        return {"run_id": run_id, "failures": [], "quarantined": []}
    report.setdefault("failures", [])
    report.setdefault("quarantined", [])
    return report


def record(
    run_id: str,
    failures: list[dict[str, str]],
    quarantined: list[str] = (),
    path: Path = REPORT_PATH,
) -> dict[str, Any]:
    """Add failures (each {stage, name, detail}) and marts to hold back to this run's report."""
    report = read(run_id, path)
    report["failures"] = report["failures"] + list(failures)
    report["quarantined"] = sorted(set(report["quarantined"]) | set(quarantined))
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(".building")
    staged.write_text(json.dumps(report, indent=2))
    staged.replace(path)
    return report


def reset(run_id: str, path: Path = REPORT_PATH) -> None:
    """Start this run's report empty. Called by the first step of the run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(".building")
    staged.write_text(json.dumps({"run_id": run_id, "failures": [], "quarantined": []}))
    staged.replace(path)


def summary(report: dict[str, Any]) -> str:
    lines = [f"{f['stage']}: {f['name']} -- {f['detail']}" for f in report.get("failures", [])]
    held = report.get("quarantined", [])
    text = f"{len(lines)} failure(s) in analytics_hourly; everything else was built and published.\n"
    text += "\n".join(lines[:40])
    if len(lines) > 40:
        text += f"\n... and {len(lines) - 40} more"
    if held:
        text += "\nServed from the previous copy because their own tests failed: " + ", ".join(held)
    return text
