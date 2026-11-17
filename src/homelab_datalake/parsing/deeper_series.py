"""Deeper series for politico.fraserv.fr's Données (Phase 7b): immigration by origin, public debt, security.

What is read, and from where (every file is public, none needs a key):

* **Insee workbooks** on immigrants and descendants of immigrants (stock since 1911, origin, age, employment,
  unemployment, jobs, diplomas, living standards, housing), on the Maastricht debt (since 1978), deficit and interest, on the population at 1 January since 1901 (to put the debt per head), and on
  suspects by nationality. Each workbook is kept *as a grid*: one row per numeric cell, plus the text of every text
  cell, so a table's layout can change without breaking the lake and the renderer picks cells by their labels.
* **Insee's consumer price index** (all households, base 2015, monthly) turned into annual averages: the deflator of
  household-level figures.
* **The OECD's tax wedge** (Taxing Wages: average wage and 67 % of it, single, no child) by country and year.
* **The Banque de France's share of the State's negotiable debt held by non-residents** (through DBnomics).
* **The SSMSI's regional base** of recorded crime, by region and offence.
* **The Ministry of Justice's monthly prison statistics** (most recent workbook).

Pure functions only; assets/official_statistics.py fetches and writes.
"""

from __future__ import annotations

import csv
import io
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from .xlsx import read as read_xlsx

INSEE = "https://www.insee.fr/fr/statistiques"


@dataclass(frozen=True)
class Workbook:
    code: str
    label: str
    page: str
    url: str
    sheets: tuple[str, ...] = ()  # keep only these sheets (a monthly prison workbook has fifty)


WORKBOOKS: tuple[Workbook, ...] = (
    Workbook("insee_immigres_portrait_2025", "Insee, France, portrait social 2025 : immigrés et descendants d'immigrés (stock depuis 1911, origine)",
             f"{INSEE}/8612512", f"{INSEE}/fichier/8612512/FPORSOC25-F4.xlsx"),
    Workbook("insee_emploi_origine_2025", "Insee, enquête Emploi 2025 : inactivité, chômage et emploi des immigrés et des descendants d'immigrés par origine",
             f"{INSEE}/4195420", f"{INSEE}/fichier/4195420/demo-etran-ina-cho-desc-immig.xlsx"),
    Workbook("insee_csp_origine_2025", "Insee, enquête Emploi : catégories socioprofessionnelles des immigrés et des descendants d'immigrés par origine",
             f"{INSEE}/4195806", f"{INSEE}/fichier/4195806/demo-etran-csp-desc-immig.xlsx"),
    Workbook("insee_immfra23_age", "Insee Références, Immigrés et descendants d'immigrés, édition 2023 : âge et année d'arrivée (2020)",
             f"{INSEE}/6793228", f"{INSEE}/fichier/6793228/IMMFRA23-F3.xlsx"),
    Workbook("insee_immfra23_diplome", "Insee Références, édition 2023 : niveau de diplôme des immigrés et des descendants d'immigrés (2021)",
             f"{INSEE}/6793262", f"{INSEE}/fichier/6793262/IMMFRA23-F20.xlsx"),
    Workbook("insee_immfra23_niveau_de_vie", "Insee Références, édition 2023 : niveau de vie et pauvreté monétaire selon l'ascendance migratoire (2019)",
             f"{INSEE}/6793278", f"{INSEE}/fichier/6793278/IMMFRA23-F28.xlsx"),
    Workbook("insee_immfra23_logement", "Insee Références, édition 2023 : conditions de logement selon l'ascendance migratoire (2020)",
             f"{INSEE}/6793286", f"{INSEE}/fichier/6793286/IMMFRA23-F32.xlsx"),
    Workbook("insee_dette_instruments", "Insee, dette des administrations publiques au sens de Maastricht en 2025 : par instrument financier",
             "https://www.insee.fr/en/statistiques/8988914", f"{INSEE}/fichier/8988914/t_3102_en.xlsx".replace("/fr/", "/en/")),
    Workbook("insee_dette_nette", "Insee, dette nette des administrations publiques et actifs liquides (2025)",
             "https://www.insee.fr/en/statistiques/8988914", f"{INSEE}/fichier/8988914/t_3103_en.xlsx".replace("/fr/", "/en/")),
    Workbook("insee_deficit_apu", "Insee, déficit des administrations publiques au sens de Maastricht depuis 1949 (2025)",
             "https://www.insee.fr/en/statistiques/8988914", f"{INSEE}/fichier/8988914/t_3106_en.xlsx".replace("/fr/", "/en/")),
    Workbook("insee_apu_depenses_recettes", "Insee, comptes des administrations publiques : dépenses et recettes depuis 1959 (tableau 3.201, 2024)",
             f"{INSEE}/8574705", f"{INSEE}/fichier/8574705/t_3201.xlsx"),
    Workbook("insee_dette_longue", "Insee, dette des administrations publiques au sens de Maastricht par sous-secteur, 1978-2025 (comptes nationaux base 2020, en Md€ et en % du PIB)",
             f"{INSEE}/2830192", f"{INSEE}/fichier/2830192/econ-fin-pub-dette-adm-pub.xlsx"),
    Workbook("insee_population_longue", "Insee, bilan démographique 2025, tableaux rétrospectifs : population au 1er janvier (France depuis 1982, France métropolitaine depuis 1901)",
             f"{INSEE}/8721185", f"{INSEE}/fichier/8721185/1_Pop_annu_compo_evol.xlsx", sheets=("FR", "FM")),
    # Origins of immigration and of births (parsing/insee_origins.py reads the département tables)
    Workbook("insee_immigres_pays_1968_2019", "Insee, recensements : immigrés par pays de naissance détaillé, 1968-1999 (recensements) et 2006-2019 (annuel)",
             f"{INSEE}/6478091", f"{INSEE}/fichier/6478091/pays_naissance_detaille_1968_2019.xlsx", sheets=("2006-2019", "1968-1999")),
    Workbook("insee_immigres_groupes_2006_2025", "Insee, recensements : immigrés par groupe de pays de naissance, 2006-2025 (effectifs en milliers)",
             f"{INSEE}/2381755", f"{INSEE}/fichier/2381755/demo-etran-repart-immig-pays.xlsx", sheets=("Figure 1",)),
    Workbook("insee_naissances_pays_mere_nat", "Insee, état civil, Les naissances en 2025 : T38BIS, naissances vivantes selon le pays de naissance de la mère, 1977-2025",
             f"{INSEE}/9000183", f"{INSEE}/fichier/9000183/t38bis.xlsx", sheets=("FM", "FR")),
    Workbook("insee_naissances_dep_pays_mere_2025", "Insee, état civil, Les naissances en 2025 : N7DBIS, naissances vivantes selon le pays de naissance de la mère par département",
             f"{INSEE}/9000191", f"{INSEE}/fichier/9000191/n7dbis-v2.xlsx"),
    Workbook("insee_securite_auteurs", "Insee Références, Sécurité : mis en cause selon la nationalité, réponse pénale et peines (2019)",
             f"{INSEE}/5763585", f"{INSEE}/fichier/5763585/IREF_SECUR21-F21.xlsx"),
)

IPC_SERIES = "001759970"
IPC_URL = f"https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/{IPC_SERIES}"
IPC_PAGE = f"https://www.insee.fr/fr/statistiques/serie/{IPC_SERIES}"
OECD_URL = ("https://sdmx.oecd.org/public/rest/data/OECD.CTP.TPS,DSD_TAX_WAGES_COMP@DF_TW_COMP,1.0/"
            ".AV_TW.PT_COS_LB.S_C0.AW100+AW67._Z.A?format=csv")
OECD_PAGE = "https://www.oecd.org/en/data/datasets/oecd-tax-wage-comparing-tool.html"
BDF_URL = "https://api.db.nomics.world/v22/series/BDF/DET/Q.FR.1315.F33000.M.Z9.8.F?observations=1"
BDF_PAGE = "https://db.nomics.world/BDF/DET/Q.FR.1315.F33000.M.Z9.8.F"
SSMSI_DATASET_API = "https://www.data.gouv.fr/api/1/datasets/621df2954fa5a3b5a023e23c/"
JUSTICE_API = "https://www.data.gouv.fr/api/1/datasets/5369a048a3a729239d206322/"
JUSTICE_PAGE = "https://www.data.gouv.fr/datasets/5369a048a3a729239d206322"
JUSTICE_SHEETS = ("Tab2", "Tab6", "Tab11", "Chiffres_cles")

ISO3_TO_GEO = {"AUT": "AT", "BEL": "BE", "CZE": "CZ", "DNK": "DK", "EST": "EE", "FIN": "FI", "FRA": "FR", "DEU": "DE", "GRC": "EL",
               "HUN": "HU", "IRL": "IE", "ITA": "IT", "LVA": "LV", "LTU": "LT", "LUX": "LU", "NLD": "NL", "POL": "PL", "PRT": "PT",
               "SVK": "SK", "SVN": "SI", "ESP": "ES", "SWE": "SE"}


# ---------------------------------------------------------------- workbooks as grids

def grid_rows(content: bytes, sheets: tuple[str, ...] = ()) -> tuple[list[dict], dict[str, str]]:
    """Numeric cells as rows, text cells as labels, both keyed by sheet, row and column (0-based columns)."""
    numbers: list[dict] = []
    texts: dict[str, str] = {}
    for name, rows in read_xlsx(content).items():
        if sheets and name not in sheets:
            continue
        for number, cells in rows:
            for col, cell in enumerate(cells):
                if isinstance(cell, float):
                    numbers.append({"sheet": name, "row": str(number), "col": str(col), "time": "0", "value": cell})
                elif isinstance(cell, str) and cell.strip():
                    texts[f"{name}|{number}|{col}"] = cell.strip()
    if not numbers:
        raise ValueError("the workbook has no numeric cell: its layout changed")
    return numbers, texts


def grid_block(wb: Workbook, content_rows: list[dict], texts: dict[str, str], fetched_at: str) -> dict:
    dims = ["sheet", "row", "col"]
    return {"source": "workbook", "code": wb.code, "label": wb.label, "updated": None, "url": wb.page, "file": wb.url,
            "fetched_at": fetched_at, "status": "ok", "dims": dims, "labels": {"text": texts},
            "rows": [[r["sheet"], r["row"], r["col"], r["value"]] for r in content_rows]}


# ---------------------------------------------------------------- consumer price index

def ipc_annual(xml_text: str) -> list[dict]:
    """Annual average of the monthly index, only for the years with their twelve months."""
    root = ET.fromstring(xml_text)
    months: dict[str, list[float]] = {}
    for obs in root.iter():
        if obs.tag.endswith("Obs") and obs.get("TIME_PERIOD") and obs.get("OBS_VALUE"):
            period = obs.get("TIME_PERIOD")
            if re.fullmatch(r"\d{4}-\d{2}", period):
                months.setdefault(period[:4], []).append(float(obs.get("OBS_VALUE")))
    out = [{"series": "ipc_menages_base_2015", "time": y, "value": sum(v) / 12} for y, v in sorted(months.items()) if len(v) == 12]
    if len(out) < 20:
        raise ValueError("the Insee price index has fewer than twenty complete years")
    return out


# ---------------------------------------------------------------- OECD tax wedge

def oecd_wedge_rows(csv_text: str) -> list[dict]:
    """Tax wedge (% of labour cost), average rate, single no child, by country (Eurostat codes) and year, at 100 % and 67 %
    of the average wage; plus the simple mean of the EU countries the OECD covers as `EU27_2020`."""
    rows: list[dict] = []
    by_year: dict[tuple[str, str], list[float]] = {}
    for r in csv.DictReader(io.StringIO(csv_text)):
        geo = ISO3_TO_GEO.get(r["REF_AREA"])
        if geo is None or not r["OBS_VALUE"]:
            continue
        income = r["INCOME_PRINCIPAL"]
        rows.append({"income": income, "geo": geo, "time": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])})
        by_year.setdefault((income, r["TIME_PERIOD"]), []).append(float(r["OBS_VALUE"]))
    if not rows:
        raise ValueError("the OECD tax wedge query returned nothing")
    rows += [{"income": i, "geo": "EU27_2020", "time": t, "value": sum(v) / len(v)} for (i, t), v in by_year.items() if len(v) >= 18]
    return rows


# ---------------------------------------------------------------- Banque de France, through DBnomics

def bdf_rows(payload: dict) -> list[dict]:
    doc = (payload.get("series", {}).get("docs") or [None])[0]
    if not doc:
        raise ValueError("DBnomics returned no Banque de France series")
    return [{"series": "non_residents_negotiable_debt_pct", "time": p, "value": float(v)}
            for p, v in zip(doc["period"], doc["value"], strict=True) if isinstance(v, (int, float))]


# ---------------------------------------------------------------- SSMSI regional base

def ssmsi_regional_rows(csv_text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("﻿")), delimiter=";")
    need = {"Code_region", "annee", "indicateur", "unite_de_compte", "nombre", "taux_pour_mille", "insee_pop"}
    if need - set(reader.fieldnames or ()):
        raise ValueError(f"SSMSI regional file: columns {sorted(need - set(reader.fieldnames or ()))} are gone")

    def num(x: str) -> float | None:
        x = (x or "").strip().replace(",", ".")
        return float(x) if x and x.upper() != "NA" else None

    out = []
    for r in reader:
        for measure, key in (("nombre", "nombre"), ("taux_pour_mille", "taux_pour_mille"), ("population", "insee_pop")):
            v = num(r[key])
            if v is not None:
                out.append({"indicateur": r["indicateur"].strip(), "unite": r["unite_de_compte"].strip(), "mesure": measure,
                            "geo": r["Code_region"].strip(), "time": r["annee"].strip(), "value": v})
    return out


def ssmsi_regional_resource(dataset: dict) -> dict:
    cands = [r for r in dataset.get("resources", []) if (r.get("format") or "").lower() == "csv" and (r.get("title") or "").startswith("REG ")]
    if not cands:
        raise ValueError("the SSMSI dataset no longer lists a regional CSV (title 'REG - ...')")
    return max(cands, key=lambda r: r.get("last_modified") or "")


# ---------------------------------------------------------------- Ministry of Justice, monthly prison statistics

def justice_latest(dataset: dict) -> dict:
    """The most recent monthly workbook: `statistique-etablissements-DDMMYYYY.xlsx`."""
    best, best_date = None, ""
    for r in dataset.get("resources", []):
        m = re.search(r"statistique-etablissements-(\d{2})(\d{2})(\d{4})\.xlsx$", r.get("url") or "")
        if m and f"{m.group(3)}{m.group(2)}{m.group(1)}" > best_date:
            best, best_date = r, f"{m.group(3)}{m.group(2)}{m.group(1)}"
    if not best:
        raise ValueError("the Justice dataset no longer lists a monthly workbook named statistique-etablissements-DDMMYYYY.xlsx")
    return best
