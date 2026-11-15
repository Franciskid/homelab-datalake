"""Public spending and pensions: the non-Eurostat inputs of politico.fraserv.fr's "Dépenses publiques".

Three French publishers, each read as an open-data CSV export:

* **The State budget by programme and category** (data.economie.gouv.fr, one dataset per
  finance bill). Only the three personnel categories are kept: 21 pay, 22 social
  contributions (the employer's share of the CAS Pensions is the bulk of it), 23 social
  benefits. This is the key that tells which mission (hence which COFOG function) the State's
  pension contributions sit in. One edition per year; the editions the open-data portal
  publishes in a structured form are 2019 (LFI), 2023 (LFI), 2024 and 2025 (PLF).
* **The revenue lines of the CAS Pensions** (same portal): employers' contributions of the
  State's civil servants and military, and employees' deductions. Forecasts of the finance
  bill of the year, not outturn.
* **The DREES "Comptes de la protection sociale"**: old-age and survivors' benefits by scheme,
  1959 onwards, which gives the composition of what is paid in pensions.

Pure functions only; assets/official_statistics.py fetches. Nothing is computed here beyond
summing duplicates: the allocation of contributions to functions is an assumption and lives
with the method in the donnees renderer.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Iterable
from dataclasses import dataclass

ODS_EXPORT = "https://{host}/api/explore/v2.1/catalog/datasets/{dataset}/exports/csv?delimiter=%3B"
ODS_PAGE = "https://{host}/explore/dataset/{dataset}/"
ECONOMIE = "data.economie.gouv.fr"
DREES = "data.drees.solidarites-sante.gouv.fr"

BUDGET_CODE = "state_budget_personnel"
CAS_CODE = "cas_pensions_recettes"
CPS_CODE = "cps_vieillesse_survie"

PERSONNEL_CATEGORIES = frozenset({21, 22, 23})
MISSION_TYPES = {"budget general": "BG", "budgets annexes": "BA", "budget annexe": "BA",
                 "compte d'affectation speciale": "CAS", "comptes d'affectation speciale": "CAS",
                 "compte de concours financiers": "CCF", "comptes de concours financiers": "CCF"}


@dataclass(frozen=True)
class BudgetEdition:
    year: str
    dataset: str
    mission_type: str  # column holding BG / BA / CAS / CCF, or the long label (2019)
    programme: str
    label: str
    category: str
    credits: tuple[str, ...]  # payment-credit columns, summed (the 2023 file splits T2 / hors T2)
    vintage: str  # what the figures are: the finance bill, or the enacted law


BUDGET_EDITIONS: tuple[BudgetEdition, ...] = (
    BudgetEdition("2019", "loi-de-finances-initiale-pour-2019-lfi-2019-1", "type_de_budget_hors_budgets_annexes",
                  "code_programme", "programme", "code_categorie", ("cp_lfi_2019",), "LFI"),
    BudgetEdition("2023", "credits-ae-et-cp-votes-nomenclature-par-destination-et-nature-lfi-2023", "type_mission",
                  "programme", "libelle_programme", "categorie",
                  ("cp_t2_plf_2023", "cp_hors_t2_plf_2023", "cp_t2_amendements_2023", "cp_hors_t2_amendements_2023"),
                  "LFI"),
    BudgetEdition("2024", "plf-2024-depenses-2024-selon-nomenclatures-destination-et-nature", "type_mission",
                  "programme", "libelle_programme", "categorie", ("cp_plf",), "PLF"),
    BudgetEdition("2025", "plf25-depenses-2025-du-bg-et-des-ba-selon-nomenclatures-destination-et-nature",
                  "type_mission", "programme", "libelle_programme", "categorie", ("cp_plf",), "PLF"),
)


@dataclass(frozen=True)
class CasEdition:
    year: str
    dataset: str
    amount: str  # the column that holds the year's amount
    vintage: str


CAS_EDITIONS: tuple[CasEdition, ...] = (
    CasEdition("2023", "cas-ccf-recettes-lf-2023", "montant_recettes_lfi_2023", "LFI"),
    CasEdition("2024", "plf-2024-recettes-des-cas-et-ccf", "montant_recettes_lfi_2024", "LFI"),
    CasEdition("2025", "plf25-recettes-des-cas-et-des-ccf", "montant_recettes_plf_2025", "PLF"),
)

CAS_SECTION = "YD-1"  # "Pensions civiles et militaires de retraite et allocations temporaires d'invalidité"
CPS_PREFIX = "E11-2"  # old-age and survivors' benefits


def export_url(host: str, dataset: str) -> str:
    return ODS_EXPORT.format(host=host, dataset=dataset)


def page_url(host: str, dataset: str) -> str:
    return ODS_PAGE.format(host=host, dataset=dataset)


def _reader(text: str) -> csv.DictReader:
    return csv.DictReader(io.StringIO(text.lstrip("﻿")), delimiter=";")


def _need(reader: csv.DictReader, columns: Iterable[str], what: str) -> None:
    missing = set(columns) - set(reader.fieldnames or ())
    if missing:
        raise ValueError(f"{what}: columns {sorted(missing)} are gone from the file")


def _number(text: str | None) -> float:
    text = (text or "").strip().replace(",", ".")
    return float(text) if text else 0.0


def _integer(text: str | None) -> int | None:
    try:
        return int(float((text or "").strip()))
    except ValueError:
        return None


def mission_type(raw: str) -> str:
    key = (raw or "").strip().lower().replace("é", "e").replace("’", "'")
    return MISSION_TYPES.get(key, (raw or "").strip().upper()[:3])


# ---------------------------------------------------------------- State budget

def budget_rows(csv_text: str, edition: BudgetEdition) -> list[dict]:
    """Payment credits of the personnel categories, summed by mission, programme and category."""
    reader = _reader(csv_text)
    _need(reader, ("mission", edition.mission_type, edition.programme, edition.label, edition.category,
                   *edition.credits), f"state budget {edition.year}")
    totals: dict[tuple, float] = {}
    for row in reader:
        category = _integer(row[edition.category])
        programme = _integer(row[edition.programme])
        if category not in PERSONNEL_CATEGORIES or programme is None:
            continue
        key = (mission_type(row[edition.mission_type]), row["mission"].strip(), programme,
               row[edition.label].strip(), category)
        totals[key] = totals.get(key, 0.0) + sum(_number(row[c]) for c in edition.credits)
    return [{"time": edition.year, "type": t, "mission": m, "programme": str(p), "label": lab,
             "category": str(c), "value": v} for (t, m, p, lab, c), v in sorted(totals.items())]


def budget_block(rows: list[dict], editions: list[BudgetEdition], fetched_at: str) -> dict:
    dims = ["type", "mission", "programme", "label", "category", "time"]
    return {
        "source": "budget", "code": BUDGET_CODE, "label": "Budget de l’État : dépenses de personnel par programme",
        "updated": None, "url": page_url(ECONOMIE, editions[-1].dataset),
        "editions": {e.year: {"dataset": e.dataset, "vintage": e.vintage, "url": page_url(ECONOMIE, e.dataset)}
                     for e in editions},
        "fetched_at": fetched_at, "status": "ok", "dims": dims, "labels": {},
        "rows": [[r[d] for d in dims] + [r["value"]] for r in rows],
    }


# ---------------------------------------------------------------- CAS Pensions revenue

def cas_rows(csv_text: str, edition: CasEdition) -> list[dict]:
    reader = _reader(csv_text)
    _need(reader, ("code_section", "code_ligne_de_recette", "libelle_ligne_de_recette", edition.amount),
          f"CAS revenue {edition.year}")
    return [{"time": edition.year, "line": row["code_ligne_de_recette"].strip(),
             "label": row["libelle_ligne_de_recette"].strip(), "value": _number(row[edition.amount])}
            for row in reader if row["code_section"].strip() == CAS_SECTION]


def cas_block(rows: list[dict], editions: list[CasEdition], fetched_at: str) -> dict:
    dims = ["line", "label", "time"]
    return {
        "source": "budget", "code": CAS_CODE,
        "label": "CAS Pensions : recettes de la section des pensions civiles et militaires",
        "updated": None, "url": page_url(ECONOMIE, editions[-1].dataset),
        "editions": {e.year: {"dataset": e.dataset, "vintage": e.vintage, "url": page_url(ECONOMIE, e.dataset)}
                     for e in editions},
        "fetched_at": fetched_at, "status": "ok", "dims": dims, "labels": {},
        "rows": [[r[d] for d in dims] + [r["value"]] for r in rows],
    }


# ---------------------------------------------------------------- DREES protection sociale

CPS_DATASET = "305_les-comptes-de-la-protection-sociale"


def cps_rows(csv_text: str) -> list[dict]:
    """Old-age and survivors' benefits (ps_code E11-2...) at every level of the nomenclature.

    `val` is in million euros, current prices. The scheme is `regime_nom` under the
    institutional sector `si_code`; a "Total ..." row is the sector's own sum.
    """
    reader = _reader(csv_text)
    _need(reader, ("annee", "ps_niveau", "ps_code", "ps_lib", "regime_nom", "si_niveau", "si_code", "si_nom", "val"),
          "DREES protection sociale")
    out = []
    for row in reader:
        code = row["ps_code"].strip()
        if not code.startswith(CPS_PREFIX) or not row["val"].strip():
            continue
        out.append({"time": row["annee"].strip(), "ps_code": code, "ps_lib": row["ps_lib"].strip(),
                    "ps_level": row["ps_niveau"].strip(), "si_code": row["si_code"].strip(),
                    "si_name": row["si_nom"].strip(), "si_level": row["si_niveau"].strip(),
                    "regime": row["regime_nom"].strip(), "value": _number(row["val"])})
    return out


def cps_block(rows: list[dict], fetched_at: str, updated: str | None) -> dict:
    dims = ["ps_code", "ps_lib", "si_level", "si_code", "si_name", "regime", "time"]
    return {
        "source": "drees", "code": CPS_CODE, "label": "Comptes de la protection sociale : vieillesse et survie",
        "updated": updated, "url": page_url(DREES, CPS_DATASET), "unit": "M€ courants",
        "fetched_at": fetched_at, "status": "ok", "dims": dims, "labels": {},
        "rows": [[r[d] for d in dims] + [r["value"]] for r in rows],
    }


# ---------------------------------------------------------------- ledger

def ledger_rows(source: str, dataset: str, rows: list[dict], dims: list[str], url: str,
                label: str, updated: str | None) -> list[dict]:
    """The same rows in the lake's long format: one value per (series, period); geo is FR."""
    out = []
    for r in rows:
        coords = {d: r[d] for d in dims if d != "time"}
        out.append({
            "source": source, "dataset": dataset,
            "series_key": ";".join(f"{k}={v}" for k, v in coords.items()),
            "geo": "FR", "period": r["time"], "value": r["value"], "status": None, "unit": None,
            "dims_json": json.dumps(coords, sort_keys=True, ensure_ascii=False),
            "dataset_label": label, "source_updated": updated, "source_url": url,
        })
    return out


# ================================================================ pages on pensions: contributions, ratio, average pension
# Sources added with the pages "ressources des retraites", "cotisants par retraité", "cotisation moyenne" and "cotisations
# sociales des travailleurs": the COR's data files, the Service des retraites de l'État's executed CAS Pensions, the DREES's
# long series of social protection, and the PAP/CCSS values kept in curated_pensions.py.

import re  # noqa: E402
from html import unescape  # noqa: E402

from . import curated_pensions  # noqa: E402
from .xlsx import read as read_xlsx  # noqa: E402

COR_REPORT_PAGE = "https://www.cor-retraites.fr/rapports-du-cor/rapport-annuel-cor-juin-2026-evolutions-perspectives-retraites-france"
COR_CODE = "cor_systeme_de_retraite"
COR_STRUCTURE_CODE = "cor_structure_ressources"
SRE_CODE = "sre_cas_pensions_execution"
SRE_URL = "https://retraitesdeletat.gouv.fr/statistiques/cas.html"
CPS_RESSOURCES_CODE = "cps_ressources"
CPS_DATAGOUV = "https://www.data.gouv.fr/api/1/datasets/les-comptes-de-la-protection-sociale/"
PAP_CODE = "pap_cas_pensions_par_programme"
CCSS_CODE = "ccss_cotisations_patronales"


def cor_links(page_html: str) -> dict[str, str]:
    """The COR report page's data files by what they are: part2 (resources) and complementary (workers, pensions)."""
    out = {}
    for href, title in re.findall(r'<a href="([^"]+\.xlsx)" title="([^"]+)"', page_html):
        t = unescape(title).lower()
        url = "https://www.cor-retraites.fr" + href if href.startswith("/") else href
        if "partie 2" in t:
            out["part2"] = url
        elif "complémentaires" in t or "complementaires" in t:
            out["complementary"] = url
    for href in re.findall(r'href="([^"]+Donn[^"]*mentaires\.xlsx)"', page_html):
        out.setdefault("complementary", "https://www.cor-retraites.fr" + href if href.startswith("/") else href)
    return out


def _years_row(rows, upto: int) -> dict[int, int]:
    """Column -> year, from the last row before `upto` with many years in it."""
    best = {}
    for n, r in rows:
        if n >= upto:
            break
        cols = {i: int(c) for i, c in enumerate(r) if isinstance(c, float) and 1950 <= c <= 2100 and c == int(c)}
        if len(cols) >= 8:
            best = cols
    return best


def cor_block_rows(sheet: list[tuple[int, list]], prefix: str) -> list[dict]:
    """Series of a COR sheet in blocks: a label, then one row per scenario (`Obs`, `Sc. Ref`, ...) of yearly values."""
    out, label, unit = [], None, ""
    for n, r in sheet:
        cells = [c for c in r[:3]]
        k = next((i for i, c in enumerate(cells) if c in ("Obs", "Sc. Ref")), None)
        if k is None:
            if len(r) > 1 and isinstance(r[1], str) and r[1].startswith("En "):
                unit = f" [{r[1].strip()}]"
                continue
            if len(r) > 1 and isinstance(r[1], str) and r[1].strip() and not r[1].startswith(("Retour", "Champ", "Source", "Note", "Lecture", "Scénario")):
                label = r[1].strip()
            continue
        if k >= 1 and isinstance(cells[k - 1], str) and cells[k - 1].strip():
            label = cells[k - 1].strip()
        if label is None:
            continue
        years = _years_row(sheet, n)
        for col, year in years.items():
            if col < len(r) and isinstance(r[col], float):
                out.append({"series": f"{prefix}{label}{unit}", "scenario": cells[k], "time": str(year), "value": r[col]})
    return out


def cor_structure_rows(sheet: list[tuple[int, list]]) -> list[dict]:
    """Figure 2.11: share of each kind of resource in the pension system's resources, per year."""
    out = []
    for n, r in sheet:
        years = _years_row(sheet, n)
        text = next((c for c in r[:2] if isinstance(c, str) and c.strip()), None)
        if not years or text is None or n < 5:
            continue
        label = text.strip()
        if not label or label.startswith(("Lecture", "Champ", "Sources")):
            continue
        for col, year in years.items():
            if col < len(r) and isinstance(r[col], float):
                out.append({"series": label, "scenario": "Obs", "time": str(year), "value": r[col]})
    return out


def cor_rows(part2: bytes, complementary: bytes) -> tuple[list[dict], list[dict]]:
    p2, comp = read_xlsx(part2), read_xlsx(complementary)
    system = []
    system += cor_block_rows(p2["Fig 2.8"], "")
    for r in cor_block_rows(p2["Fig 2.13"], ""):
        system.append(r)
    for name in ("Cotisants_Retraités", "Rému_pensions", "Prestations"):
        system += cor_block_rows(comp[name], "")
    return system, cor_structure_rows(p2["Fig 2.11"])


def cor_block(rows: list[dict], code: str, label: str, fetched_at: str, dims: list[str]) -> dict:
    return {"source": "cor", "code": code, "label": label, "updated": None, "url": COR_REPORT_PAGE, "fetched_at": fetched_at,
            "status": "ok", "dims": dims, "labels": {}, "rows": [[r[d] for d in dims] + [r["value"]] for r in rows]}


def sre_rows(page_html: str) -> list[dict]:
    """Executed CAS Pensions (programme 741) revenue and the employer / agent rates, 2021-2025."""
    text = re.sub(r"<script.*?</script>|<style.*?</style>", " ", page_html, flags=re.S)
    text = re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", text)))
    num = r"(-?\d{1,3}(?: \d{3})*,\d{2})"
    five = r"\s+".join([num] * 5)

    block = text[text.index("741 - PCMR et ATI"):] if "741 - PCMR et ATI" in text else text
    years_m = re.search(r"Équilibre Exécution budgétaire\* ((?:20\d\d ?){5})", text)
    years = years_m.group(1).split() if years_m else ["2021", "2022", "2023", "2024", "2025"]
    items = {}

    def grab_in(t: str, pattern: str):
        m = re.search(pattern + r"\s+" + five, t)
        return [float(x.replace(" ", "").replace(",", ".")) for x in m.groups()] if m else None

    items["cotisations_salariales"] = grab_in(block, r"Cotisations salariales")
    items["contributions_employeurs"] = grab_in(block, r"Contributions employeurs")
    after = block[block.index("Contributions employeurs"):] if "Contributions employeurs" in block else block
    items["contributions_employeurs_etat"] = grab_in(after, r"dont État")
    items["taux_contribution_civils_pct"] = grab_in(text, r"au titre des pensions civiles")
    items["taux_contribution_militaires_pct"] = grab_in(text, r"au titre des pensions militaires")
    items["taux_cotisation_agent_pct"] = grab_in(text, r"Taux de cotisation agent")
    if not items["contributions_employeurs"] or not items["taux_contribution_civils_pct"]:
        raise ValueError("the Service des retraites de l'État page no longer has the CAS Pensions revenue or rates tables")
    return [{"series": k, "scenario": "Obs", "time": y, "value": v} for k, vals in items.items() if vals for y, v in zip(years, vals, strict=True)]


def cps_ressources_rows(workbook: bytes) -> list[dict]:
    """Resources and uses of social protection, all risks, in million euros (opérations sheet), 1959 onwards."""
    sheets = read_xlsx(workbook)
    name = next((n for n in sheets if n.lower().startswith("opérations_19") or n.lower().startswith("operations_19")), None)
    if not name:
        raise ValueError("the DREES workbook has no 'opérations_19xx_20xx' sheet")
    rows = sheets[name]
    header = next(r for _, r in rows if any(isinstance(c, str) and re.fullmatch(r"\d{4}[A-Z]*", c) for c in r))
    cols = {i: c[:4] for i, c in enumerate(header) if isinstance(c, str) and re.fullmatch(r"\d{4}[A-Z]*", c)}
    out = []
    for _, r in rows:
        label = r[0] if r and isinstance(r[0], str) else None
        if not label or label.startswith(("*", "Source")) or label == "":
            continue
        for col, year in cols.items():
            if col < len(r) and isinstance(r[col], float):
                out.append({"series": label.strip(), "scenario": header[col][4:] or "DEF", "time": year, "value": r[col]})
    return out


def cps_workbook_url(datagouv_json: dict) -> tuple[str, str]:
    """The newest 'La protection sociale en France de 1959 à AAAA' workbook among the dataset's resources."""
    cands = [r for r in datagouv_json.get("resources", [])
             if (r.get("title") or "").startswith("La protection sociale en France de 1959") and (r.get("format") or "").endswith("sheet")]
    if not cands:
        raise ValueError("data.gouv.fr no longer lists the DREES long-series workbook")
    best = max(cands, key=lambda r: ((r.get("title") or ""), r.get("last_modified") or ""))
    return best["url"], best.get("last_modified") or ""


def pap_rows() -> list[dict]:
    out = []
    for edition, mission, programme, year, cat22, cas, civ, mil, page, url in curated_pensions.PAP_CAS_PENSIONS:
        for item, v in (("cotisations_sociales", cat22), ("contributions_equilibre_cas", cas), ("civils", civ), ("militaires", mil)):
            if v is not None:
                out.append({"edition": edition, "mission": mission, "programme": programme, "item": item, "page": str(page), "time": year, "value": float(v)})
    return out


def pap_block(fetched_at: str) -> dict:
    dims = ["edition", "mission", "programme", "item", "page", "time"]
    rows = pap_rows()
    urls = {r[0] + "|" + r[1]: r[9] for r in curated_pensions.PAP_CAS_PENSIONS}
    return {"source": "curated", "code": PAP_CODE, "label": "PAP : contributions d'équilibre au CAS Pensions par programme", "updated": None,
            "url": "https://www.assemblee-nationale.fr/dyn/17/budget", "fetched_at": fetched_at, "status": "ok", "dims": dims,
            "labels": {}, "files": urls, "rows": [[r[d] for d in dims] + [r["value"]] for r in rows]}


def ccss_block(fetched_at: str) -> dict:
    dims = ["series", "edition", "page", "url", "time"]
    rows = [[sch, rep, str(pg), url, y, float(v) * 1e6] for sch, y, v, rep, pg, url in curated_pensions.CCSS_COTISATIONS_PATRONALES]
    rows += [["CNRACL taux employeur (%)", src, "", url, y, float(v)] for y, v, src, url in curated_pensions.CNRACL_TAUX_EMPLOYEUR]
    rows += [[f"{sch} {kind}", "CCSS octobre 2024", str(pg), "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf", y, float(n)]
             for sch, kind, y, n, pg in curated_pensions.CCSS_EFFECTIFS]
    return {"source": "curated", "code": CCSS_CODE, "label": "CCSS : cotisations patronales de la CNRACL et de l'Ircantec (euros) et taux employeur CNRACL (%)",
            "updated": None, "url": "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf", "fetched_at": fetched_at, "status": "ok",
            "dims": dims, "labels": {}, "rows": rows}
