"""Collector for the SSMSI's annual bilan and its Info rapide on domestic violence (the formats and checks are in parsing/ssmsi_bilan.py).

Where each input comes from:

* the bilan **data tables** (zip) and the Info rapide files are **manual inputs** (manual_inputs.py): the first answers 403 to scripts, the others
  have no address that can be guessed. They are renewed by hand: the zip each July, the Info rapide each November.
* the bilan **PDF** is fetched by a plain request, once a year, from the files host (statistiques.interieur.gouv.fr/ssmsi/files), which serves it. The
  request carries the same honest client name as every other collector in this datalake. If the host refuses or does not answer, that is logged and the
  stored copy is kept: nothing here retries, rotates a client name or goes through another route.

The PDF is not where the figures come from: it is what they are checked against (parsing.check_against_pdf), and it gives the population share
the SSMSI states. A figure that disagrees with the PDF fails the block (the export then keeps yesterday's, marked stale); a PDF whose layout has
changed only skips that check, with a warning.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime
from urllib.parse import quote

from homelab_datalake import manual_inputs as manual
from homelab_datalake.parsing import crime_nationality as c
from homelab_datalake.parsing import ssmsi_bilan as s
from homelab_datalake.resources import LakeStorage

SOURCE = "ssmsi"
USER_AGENT = "fraserv-datalake/1.0 (official statistics)"
TIMEOUT_SECONDS = 90
PROBE_EVERY_DAYS = 7
MAX_PDF_BYTES = 80_000_000


def plain_get(url: str) -> tuple[int, bytes]:
    """One request, no retry: (status, body). A refusal is an answer, not an error to get round; a network failure is status 0."""
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https source: {url!r}")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})  # noqa: S310
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310
            return response.status, response.read(MAX_PDF_BYTES + 1)
    except urllib.error.HTTPError as error:
        return error.code, b""
    except (urllib.error.URLError, TimeoutError, OSError):
        return 0, b""


def bilan_pdf_url(edition: int, month: int) -> str:
    """The file the SSMSI published in July 2026 is « …/2026-07/Insécurité et délinquance en 2025 - Bilan statistique - édition 2026 - SSMSI.pdf »."""
    name = f"Insécurité et délinquance en {edition - 1} - Bilan statistique - édition {edition} - SSMSI.pdf"
    return f"{s.FILES_HOST}/{edition}-{month:02d}/{quote(name)}"


def probe_next_edition(lake: LakeStorage, log, today: date, get=plain_get, pause: float = 1.0) -> dict | None:
    """Once the next edition is due (June onwards), look for its PDF at the address the host uses, once a week, one plain request per month.

    A 404 says it is not published yet. Any other answer than the PDF (403, 503, a page that is not a PDF, no answer) stops the search for this week:
    it is logged and the stored copy stays. Returns the new manifest entry when a new edition was stored.
    """
    current = manual.latest(lake, SOURCE, "bilan_pdf")
    edition = (current["edition"] if current else today.year - 1) + 1
    if today < date(edition, 6, 1):
        return None
    last = manual.probed(lake, SOURCE, f"bilan_pdf:{edition}")
    if last and (today - date.fromisoformat(last)).days < PROBE_EVERY_DAYS:
        return None
    found = None
    for month in range(6, 13):
        if date(edition, month, 1) > today:
            break
        url = bilan_pdf_url(edition, month)
        status, body = get(url)
        if status == 200 and body[:5] == b"%PDF-" and len(body) <= MAX_PDF_BYTES:
            found = manual.put(lake, SOURCE, "bilan_pdf", url.rsplit("/", 1)[-1], body, edition=edition, url=url, how="fetched: plain request, official files host",
                               note="fetched by the datalake, once a year", obtained_on=today.isoformat())
            log.info("SSMSI bilan %d PDF fetched (%d bytes)", edition, len(body))
            break
        if status == 404:
            time.sleep(pause)
            continue
        log.warning("SSMSI bilan %d PDF: %s answered %s: not retried, the stored copy of edition %s is kept", edition, url.split("/ssmsi/files/")[0], status or "nothing",
                    current["edition"] if current else "none")
        break
    manual.mark_probed(lake, SOURCE, f"bilan_pdf:{edition}", today.isoformat())
    return found


def _provenance(entry: dict) -> dict:
    return {k: entry[k] for k in ("role", "edition", "name", "sha256", "url", "how", "obtained_on")}


def collect_ssmsi_bilan(fetched_at: str, log, lake: LakeStorage | None = None, only: set[str] | None = None, *, get=plain_get, today: date | None = None
                        ) -> tuple[list[dict], dict[str, dict], list[str]]:
    """The bilan's counts by nationality (mis en cause and victims) and the domestic-violence figures, as export blocks plus their ledger rows."""
    rows: list[dict] = []
    blocks: dict[str, dict] = {}
    failed: list[str] = []
    lake = lake or LakeStorage.from_env()
    today = today or datetime.now(UTC).date()

    def land(code: str, label: str, url: str, dims: list[str], parsed: list[dict], extra: dict) -> None:
        blocks[f"{c.SOURCE}:{code}"] = c.block(code, label, url, dims, [[r[d] for d in dims] + [r["value"]] for r in parsed], fetched_at, extra=extra)
        rows.extend(c.ledger(code, parsed, dims, url, label, None))

    def run(code: str, work) -> None:
        if only is not None and code not in only:
            return
        try:
            work()
        except Exception as error:  # noqa: BLE001 -- one source down must not stop the others
            log.warning("crime %s failed: %s", code, error)
            failed.append(f"{c.SOURCE}:{code}")

    def bilan() -> None:
        zip_bytes, zip_entry = manual.get(lake, SOURCE, "bilan_data_zip")
        tables = s.bilan_tables(zip_bytes)
        try:
            probe_next_edition(lake, log, today, get)
        except Exception as error:  # noqa: BLE001 -- the PDF is a check, never a reason to lose the tables
            log.warning("SSMSI bilan PDF: the search for a new edition failed (%s); the stored copy is kept", error)
        checks = dict(tables["checks"])
        inputs = [_provenance(zip_entry)]
        stated = None
        try:
            pdf_bytes, pdf_entry = manual.get(lake, SOURCE, "bilan_pdf")
            inputs.append(_provenance(pdf_entry))
            if pdf_entry["edition"] > zip_entry["edition"]:
                log.warning("SSMSI: the stored PDF is the %d edition and the stored data tables the %d edition: the zip is refreshed by hand each July "
                            "(download it in a browser from %s and put it in the lake); the PDF check is skipped until then", pdf_entry["edition"], zip_entry["edition"], s.SOURCE_PAGE)
                checks["pdf"] = f"skipped: the data tables are the {zip_entry['edition']} edition, the PDF the {pdf_entry['edition']} edition"
            else:
                try:
                    pages = s.pdf_text(pdf_bytes)
                    printed = s.pdf_profile(pages)
                    checks["pdf"] = s.check_against_pdf(tables["profile"], printed)
                    stated = s.population_stated(printed)
                except s.PdfLayoutError as error:
                    log.warning("SSMSI PDF: layout not recognised, the check against it is skipped (%s)", error)
                    checks["pdf"] = f"not run: {error}"
        except FileNotFoundError as error:
            log.warning("SSMSI: no stored bilan PDF (%s); the figures are checked against the synthèse workbook only", error)
            checks["pdf"] = "not run: no stored PDF"
        extra = {"labels": {"indicator": s.INDICATOR_LABELS, "group": s.GROUP_LABELS}, "population_foreign_pct_stated": stated, "population_groups_pct_stated": tables["population_groups"],
                 "checks": checks, "inputs": inputs, "edition": zip_entry["edition"],
                 "manual_refresh": "Les tableaux de données du bilan sont téléchargés à la main (le serveur refuse les requêtes automatiques) et renouvelés chaque juillet."}
        land("fr_mec_nat", f"SSMSI, bilan {zip_entry['edition']} (séries longues) : mis en cause selon la nationalité, 2016-{checks['mec']['years'][-4:]}", s.SOURCE_PAGE,
             s.DIMS, tables["mec"], extra)
        land("fr_vic_nat", f"SSMSI, bilan {zip_entry['edition']} (séries longues) : victimes enregistrées selon la nationalité, 2016-{checks['victims']['years'][-4:]}", s.SOURCE_PAGE,
             s.DIMS, tables["victims"], extra)

    def conjugal() -> None:
        out: list[dict] = []
        inputs: list[dict] = []
        by_year: dict[int, list[dict]] = {}
        for e in manual.read_manifest(lake, SOURCE)["entries"]:
            if e["role"] in ("ir_domestic_pdf", "ir_domestic_xlsx"):
                by_year.setdefault(e["edition"], [])
        for year in sorted(by_year):
            xlsx = manual.latest(lake, SOURCE, "ir_domestic_xlsx", year)
            if xlsx:
                data, entry = manual.get(lake, SOURCE, "ir_domestic_xlsx", year)
                got = s.ir69_rows(data, year)
            else:
                data, entry = manual.get(lake, SOURCE, "ir_domestic_pdf", year)
                got = s.ir56_rows(s.pdf_text(data), year)
            inputs.append(_provenance(entry))
            by_year[year] = got
            out.extend(r for r in got if r["measure"] != "n_revised")
        if len(by_year) < 1 or not out:
            raise ValueError("no Info rapide on domestic violence is stored")
        # The newer file re-states the previous year's counts; the older one's must be within 0.5 % of them (revisions, not another table).
        for year, got in by_year.items():
            revised = {(r["side"], r["category"]): r["value"] for r in got if r["measure"] == "n_revised"}
            for r in by_year.get(year - 1, []):
                key = (r["side"], r["category"])
                if r["measure"] == "n" and key in revised and r["value"] >= 100 and abs(revised[key] - r["value"]) > 0.005 * r["value"]:
                    raise ValueError(f"IR {year - 1} {key}: {r['value']} against {revised[key]} as re-stated in the {year} Info rapide")
        extra = {"labels": {"side": {"victims": "Victimes enregistrées", "charged": "Mis en cause"},
                            "category": {k: label for k, label, _stem in s.ir_categories()}, "measure": {"n": "Nombre", "french_pct": "Part de personnes françaises (%, entier)"}},
                 "inputs": inputs}
        land("fr_conjugal", "SSMSI, Interstats Info rapide : violences conjugales enregistrées selon la nationalité (française ou étrangère), victimes et mis en cause", s.IR_PAGE,
             ["side", "category", "time", "measure"], out, extra)

    run("fr_bilan", bilan)
    run("fr_conjugal", conjugal)
    return rows, blocks, failed
