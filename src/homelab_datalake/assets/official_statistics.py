"""Official statistics for politico.fraserv.fr: Eurostat and the SSMSI, landed and exported.

Every query in parsing/official_statistics.py is read once a day. The values land in one
ledger, `official_stat_observations`, keyed by publisher, dataset, series, place and period:
a revision by the publisher becomes a new version of the row, and the previous figure stays
queryable. Then the day's figures are written as one JSON file for politico.fraserv.fr's
renderer, which turns them into charts and pages.

The export directory is a bind mount shared with the politico stack. Without it (a test, a
laptop) the ledger still lands and the export is reported as skipped.
"""


import json
import os
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
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

from homelab_datalake import lakeio, layout
from homelab_datalake.parsing import ledger
from homelab_datalake.parsing import official_statistics as stats
from homelab_datalake.parsing import deeper_series as deeper
from homelab_datalake.parsing import public_spending as spending
from homelab_datalake.assets.crime_nationality import collect_crime
from homelab_datalake.resources import LakeStorage

LEDGER_KEY = f"{layout.prefix('official_stat_observations')}/data.parquet"
LEDGER_SCHEMA = pa.schema([(column, pa.string()) for column in stats.LEDGER_COLUMNS] + list(ledger.META_SCHEMA))
DEFAULT_EXPORT_DIR = "/exports/politico"
EXPORT_NAME = "official-statistics.json"
REQUEST_TIMEOUT_SECONDS = 180
ATTEMPTS = 3
USER_AGENT = "fraserv-datalake/1.0 (official statistics)"

CHECK_ALL_ANSWERED = "every_publisher_answered"
CHECK_EXPORTED = "export_written"


def _get(url: str) -> bytes:
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https source: {url!r}")
    last: Exception | None = None
    for attempt in range(ATTEMPTS):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})  # noqa: S310
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
                return response.read()
        except Exception as error:  # noqa: BLE001 -- retried, then re-raised with its cause
            last = error
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{url} failed {ATTEMPTS} times") from last


def fetch_eurostat(query: stats.EurostatQuery) -> dict:
    payload = json.loads(_get(query.url()))
    if "id" not in payload:
        # The API answers large requests asynchronously with a message instead of data.
        raise RuntimeError(f"{query.code}: Eurostat did not return a dataset: {str(payload)[:300]}")
    return payload


def fetch_ssmsi() -> tuple[list[dict], dict]:
    resource = stats.ssmsi_resource(json.loads(_get(stats.SSMSI_DATASET_API)))
    text = _get(resource["url"]).decode("utf-8")
    return stats.ssmsi_rows(text), resource


def fetch_text(url: str) -> str:
    return _get(url).decode("utf-8")


def collect_pension_pages(fetched_at: str, log, land) -> list[str]:
    """The COR's data, the executed CAS Pensions, the DREES long series and the curated PAP/CCSS values."""
    failed: list[str] = []
    try:
        page = fetch_text(spending.COR_REPORT_PAGE)
        links = spending.cor_links(page)
        system, structure = spending.cor_rows(_get(links["part2"]), _get(links["complementary"]))
        for code, rows_, dims, label in ((spending.COR_CODE, system, ["series", "scenario", "time"], "COR : système de retraite, cotisants, pensions moyennes"),
                                         (spending.COR_STRUCTURE_CODE, structure, ["series", "scenario", "time"], "COR : structure des ressources du système de retraite")):
            block = spending.cor_block(rows_, code, label, fetched_at, dims)
            land("cor", code, block, rows_)
    except Exception as error:  # noqa: BLE001
        log.warning("COR data failed: %s", error)
        failed += [f"cor:{spending.COR_CODE}", f"cor:{spending.COR_STRUCTURE_CODE}"]
    try:
        rows_ = spending.sre_rows(fetch_text(spending.SRE_URL))
        block = {"source": "sre", "code": spending.SRE_CODE, "label": "Service des retraites de l'État : CAS Pensions exécuté et taux de contribution",
                 "updated": None, "url": spending.SRE_URL, "fetched_at": fetched_at, "status": "ok", "dims": ["series", "scenario", "time"],
                 "labels": {}, "rows": [[r["series"], r["scenario"], r["time"], r["value"]] for r in rows_]}
        land("sre", spending.SRE_CODE, block, rows_)
    except Exception as error:  # noqa: BLE001
        log.warning("Service des retraites de l'État failed: %s", error)
        failed.append(f"sre:{spending.SRE_CODE}")
    try:
        import json as _json
        url, modified = spending.cps_workbook_url(_json.loads(fetch_text(spending.CPS_DATAGOUV)))
        rows_ = spending.cps_ressources_rows(_get(url))
        block = {"source": "drees", "code": spending.CPS_RESSOURCES_CODE, "label": "DREES, comptes de la protection sociale : ressources et emplois, tous risques (M€)",
                 "updated": modified, "url": spending.page_url(spending.DREES, spending.CPS_DATASET), "file": url, "fetched_at": fetched_at, "status": "ok",
                 "dims": ["series", "scenario", "time"], "labels": {}, "rows": [[r["series"], r["scenario"], r["time"], r["value"]] for r in rows_]}
        land("drees", spending.CPS_RESSOURCES_CODE, block, rows_)
    except Exception as error:  # noqa: BLE001
        log.warning("DREES long series failed: %s", error)
        failed.append(f"drees:{spending.CPS_RESSOURCES_CODE}")
    for code, block in ((spending.PAP_CODE, spending.pap_block(fetched_at)), (spending.CCSS_CODE, spending.ccss_block(fetched_at))):
        parsed = [dict(zip(block["dims"] + ["value"], r, strict=True)) for r in block["rows"]]
        for r in parsed:
            r.setdefault("time", r.get("time"))
        land("curated", code, block, parsed)
    return failed


def collect_deeper(fetched_at: str, log) -> tuple[list[dict], dict[str, dict], list[str]]:
    """Phase 7b inputs: Insee workbooks, price index, OECD tax wedge, Banque de France holders, SSMSI regions, Justice prisons.
    Each source is all-or-nothing and fails alone, so the export keeps yesterday's block (marked stale)."""
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []

    def land(source: str, code: str, block: dict, parsed: list[dict]) -> None:
        rows.extend(spending.ledger_rows(source, code, parsed, block["dims"], block["url"], block["label"], block.get("updated")))
        blocks[stats.dataset_key(source, code)] = block

    def table(source, code, label, url, parsed, dims, extra=None):
        block = {"source": source, "code": code, "label": label, "updated": None, "url": url, "fetched_at": fetched_at, "status": "ok",
                 "dims": dims, "labels": {}, "rows": [[r[d] for d in dims] + [r["value"]] for r in parsed], **(extra or {})}
        land(source, code, block, parsed)

    for wb in deeper.WORKBOOKS:
        try:
            cells, texts = deeper.grid_rows(_get(wb.url), wb.sheets)
            land("workbook", wb.code, deeper.grid_block(wb, cells, texts, fetched_at), cells)
        except Exception as error:  # noqa: BLE001
            log.warning("workbook %s failed: %s", wb.code, error)
            failed.append(f"workbook:{wb.code}")
    try:
        parsed = deeper.ipc_annual(fetch_text(deeper.IPC_URL))
        table("insee", "ipc_menages_annuel", "Insee, indice des prix à la consommation (ensemble des ménages, base 2015), moyenne annuelle", deeper.IPC_PAGE, parsed, ["series", "time"])
    except Exception as error:  # noqa: BLE001
        log.warning("Insee price index failed: %s", error)
        failed.append("insee:ipc_menages_annuel")
    try:
        parsed = deeper.oecd_wedge_rows(fetch_text(deeper.OECD_URL))
        table("oecd", "tax_wedge", "OCDE, Taxing Wages : coin fiscal (% du coût du travail), célibataire sans enfant", deeper.OECD_PAGE, parsed, ["income", "geo", "time"])
    except Exception as error:  # noqa: BLE001
        log.warning("OECD tax wedge failed: %s", error)
        failed.append("oecd:tax_wedge")
    try:
        import json as _json
        parsed = deeper.bdf_rows(_json.loads(fetch_text(deeper.BDF_URL)))
        table("bdf", "non_residents_debt", "Banque de France (via DBnomics) : part des non-résidents dans la dette négociable de l'État", deeper.BDF_PAGE, parsed, ["series", "time"])
    except Exception as error:  # noqa: BLE001
        log.warning("Banque de France holders failed: %s", error)
        failed.append("bdf:non_residents_debt")
    try:
        import json as _json
        resource = deeper.ssmsi_regional_resource(_json.loads(fetch_text(deeper.SSMSI_DATASET_API)))
        parsed = deeper.ssmsi_regional_rows(fetch_text(resource["url"]))
        table("ssmsi", "delinquance_regionale", resource.get("title") or "SSMSI, base régionale", stats.SSMSI_PAGE, parsed, ["indicateur", "unite", "mesure", "geo", "time"], {"file": resource["url"]})
    except Exception as error:  # noqa: BLE001
        log.warning("SSMSI regional base failed: %s", error)
        failed.append("ssmsi:delinquance_regionale")
    try:
        import json as _json
        resource = deeper.justice_latest(_json.loads(fetch_text(deeper.JUSTICE_API)))
        wb = deeper.Workbook("justice_population_detenue", "Ministère de la Justice : statistique mensuelle de la population écrouée et détenue (dernier mois)", deeper.JUSTICE_PAGE, resource["url"], deeper.JUSTICE_SHEETS)
        cells, texts = deeper.grid_rows(_get(wb.url), wb.sheets)
        land("workbook", wb.code, deeper.grid_block(wb, cells, texts, fetched_at), cells)
    except Exception as error:  # noqa: BLE001
        log.warning("Justice prison statistics failed: %s", error)
        failed.append("workbook:justice_population_detenue")
    return rows, blocks, failed


def collect_spending(fetched_at: str, log) -> tuple[list[dict], dict[str, dict], list[str]]:
    """The State budget, the CAS Pensions revenue and the DREES pensions by scheme.

    Each is all-or-nothing: if one edition of the budget cannot be read, the whole block is
    reported failed so the export keeps yesterday's complete block (marked stale) instead of
    a partial one.
    """
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []

    def land(source: str, code: str, block: dict, parsed: list[dict]) -> None:
        rows.extend(spending.ledger_rows(source, code, parsed, block["dims"], block["url"], block["label"],
                                         block.get("updated")))
        blocks[stats.dataset_key(source, code)] = block

    try:
        parsed = [r for e in spending.BUDGET_EDITIONS
                  for r in spending.budget_rows(fetch_text(spending.export_url(spending.ECONOMIE, e.dataset)), e)]
        land("budget", spending.BUDGET_CODE, spending.budget_block(parsed, list(spending.BUDGET_EDITIONS), fetched_at), parsed)
    except Exception as error:  # noqa: BLE001 -- one source down must not stop the others
        log.warning("state budget failed: %s", error)
        failed.append(f"budget:{spending.BUDGET_CODE}")
    try:
        parsed = [r for e in spending.CAS_EDITIONS
                  for r in spending.cas_rows(fetch_text(spending.export_url(spending.ECONOMIE, e.dataset)), e)]
        land("budget", spending.CAS_CODE, spending.cas_block(parsed, list(spending.CAS_EDITIONS), fetched_at), parsed)
    except Exception as error:  # noqa: BLE001
        log.warning("CAS Pensions revenue failed: %s", error)
        failed.append(f"budget:{spending.CAS_CODE}")
    failed += collect_pension_pages(fetched_at, log, land)
    try:
        parsed = spending.cps_rows(fetch_text(spending.export_url(spending.DREES, spending.CPS_DATASET)))
        land("drees", spending.CPS_CODE, spending.cps_block(parsed, fetched_at, None), parsed)
    except Exception as error:  # noqa: BLE001
        log.warning("DREES protection sociale failed: %s", error)
        failed.append(f"drees:{spending.CPS_CODE}")
    return rows, blocks, failed


def read_previous_export(directory: Path) -> dict | None:
    path = directory / EXPORT_NAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_export(directory: Path, export: dict) -> Path:
    """Write the file whole, then rename it over the old one: the renderer never reads half."""
    path = directory / EXPORT_NAME
    tmp = directory / f".{EXPORT_NAME}.tmp"
    tmp.write_text(json.dumps(export, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)
    return path


def collect(log) -> tuple[list[dict], dict[str, dict], list[str]]:
    """Read every publisher. Returns ledger rows, export blocks, and what failed."""
    fetched_at = datetime.now(UTC).isoformat(timespec="seconds")
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []
    for query in stats.EUROSTAT_QUERIES:
        try:
            payload = fetch_eurostat(query)
        except Exception as error:  # noqa: BLE001 -- one dataset down must not stop the others
            log.warning("eurostat %s failed: %s", query.key, error)
            failed.append(f"eurostat:{query.key}")
            continue
        rows.extend(stats.eurostat_ledger_rows(query, payload))
        blocks[stats.dataset_key("eurostat", query.key)] = stats.eurostat_block(query, payload, fetched_at)
    try:
        ssmsi, resource = fetch_ssmsi()
        rows.extend(stats.ssmsi_ledger_rows(ssmsi, resource))
        blocks[stats.dataset_key("ssmsi", stats.SSMSI_CODE)] = stats.ssmsi_block(ssmsi, resource, fetched_at)
    except Exception as error:  # noqa: BLE001
        log.warning("ssmsi failed: %s", error)
        failed.append(f"ssmsi:{stats.SSMSI_CODE}")
    more_rows, more_blocks, more_failed = collect_spending(fetched_at, log)
    deep_rows, deep_blocks, deep_failed = collect_deeper(fetched_at, log)
    crime_rows, crime_blocks, crime_failed = collect_crime(fetched_at, log)
    return (rows + more_rows + deep_rows + crime_rows, {**blocks, **more_blocks, **deep_blocks, **crime_blocks},
            failed + more_failed + deep_failed + crime_failed)


@asset(
    group_name="official_statistics",
    kinds={"python", "api"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    check_specs=[
        AssetCheckSpec(name=CHECK_ALL_ANSWERED, asset="official_statistics_raw",
                       description="Warns when a Eurostat query or the SSMSI file could not be read today."),
        AssetCheckSpec(name=CHECK_EXPORTED, asset="official_statistics_raw",
                       description="Warns when the export for politico.fraserv.fr was not written."),
    ],
    description=(
        "Eurostat series and the SSMSI's recorded crime, every version of every figure, plus "
        "the day's export for politico.fraserv.fr's Données section."
    ),
)
def official_statistics_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    captured_at = datetime.now(UTC)
    rows, blocks, failed = collect(context.log)
    if not rows:
        raise RuntimeError("no publisher answered; nothing to land")
    merged, merge_stats = ledger.merge(lakeio.read_strict(lake, LEDGER_KEY), rows, stats.KEY_COLUMNS,
                                       captured_at, stats.VOLATILE_COLUMNS)
    lake.write_table(LEDGER_KEY, merged)

    directory = Path(os.environ.get("OFFICIAL_STATS_EXPORT_DIR", DEFAULT_EXPORT_DIR))
    exported = None
    if directory.is_dir():
        export = stats.build_export(blocks, read_previous_export(directory),
                                    captured_at.isoformat(timespec="seconds"))
        exported = str(write_export(directory, export))
    else:
        context.log.warning("export directory %s is not mounted; export skipped", directory)

    context.log.info("official statistics: %d values read, %d new versions, %d datasets, failed: %s",
                     len(rows), merge_stats.new_versions, len(blocks), failed or "none")
    return MaterializeResult(
        metadata={"values_read": len(rows), "new_versions": merge_stats.new_versions,
                  "current_keys": merge_stats.current_keys, "datasets": len(blocks),
                  "failed": ", ".join(failed) or "none", "export": exported or "skipped"},
        check_results=[
            AssetCheckResult(check_name=CHECK_ALL_ANSWERED, passed=not failed,
                             severity=AssetCheckSeverity.WARN, metadata={"failed": ", ".join(failed) or "none"}),
            AssetCheckResult(check_name=CHECK_EXPORTED, passed=exported is not None,
                             severity=AssetCheckSeverity.WARN, metadata={"path": exported or str(directory)}),
        ],
    )


official_statistics_job = define_asset_job(
    name="official_statistics", selection=[official_statistics_raw],
    description="Read Eurostat and the SSMSI, land every figure, export them for politico.fraserv.fr.",
)


@schedule(job=official_statistics_job, cron_schedule="40 5 * * *",
          default_status=DefaultScheduleStatus.RUNNING,
          description=(
              "Daily at 05:40 UTC. Eurostat releases at 11:00 and 23:00 Luxembourg time and the "
              "SSMSI a few times a year, so one morning read is never more than a few hours late."
          ))
def official_statistics_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"official-statistics-{context.scheduled_execution_time:%Y%m%d}")


ASSETS = [official_statistics_raw]
JOBS = [official_statistics_job]
SCHEDULES = [official_statistics_schedule]
