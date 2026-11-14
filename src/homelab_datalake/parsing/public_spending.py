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
