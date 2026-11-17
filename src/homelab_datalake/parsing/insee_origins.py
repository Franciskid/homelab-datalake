"""Origins of immigration and of births, by département, from Insee's open files (Licence Ouverte).

politico.fraserv.fr's Données uses four things from here:

* **Immigrants by country of birth, census 1968 to 2025**, two workbooks kept as grids (see deeper_series.WORKBOOKS): the long
  series by detailed country (censuses 1968-1999, then every year 2006-2019) and the annual series by group of countries
  (2006-2025, which the Insee revises each spring).
* **Births by the mother's country of birth**, national (T38BIS, 1977-2025) and by département (N7DBIS): the national table
  and the 2025 département table are workbooks kept as grids; the 2024 département table, which details the mother's
  country (Algeria, Morocco, Tunisia, Ivory Coast, Congo, Guinea, Comoros), is a CSV read here.
* **Immigrants by département and country of birth, censuses 2018 and 2021** (tables IMG1B of the "Base des tableaux
  détaillés" *Immigration*), with the population of each département from IMG1A (the sum of its communes).

Pure functions; assets/official_statistics.py fetches and writes.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile

INSEE = "https://www.insee.fr/fr/statistiques"

# year -> where the census tables are. V1 splits the country of birth in 11 groups for 96 metropolitan départements; V2 in 47,
# but Insee gives it for 51 départements only.
IMG = {
    "2021": {"page": f"{INSEE}/8202714", "v1": f"{INSEE}/fichier/8202714/TD_IMG1B_V1_2021.zip",
             "v2": f"{INSEE}/fichier/8202714/TD_IMG1B_V2_2021.zip", "pop": f"{INSEE}/fichier/8202714/TD_IMG1A_2021_csv.zip"},
    "2018": {"page": f"{INSEE}/5395871", "v1": f"{INSEE}/fichier/5395871/TD_IMG1B_V1_2018.zip",
             "v2": f"{INSEE}/fichier/5395871/TD_IMG1B_V2_2018.zip", "pop": f"{INSEE}/fichier/5395871/BTT_TD_IMG1A_2018.zip"},
}
IMM_CODE = "immigres_departements"
BIRTHS_DEP_CODE = "naissances_departements_pays_mere"
BIRTHS_2024_URL = f"{INSEE}/fichier/8582142/N7DBIS.csv"
BIRTHS_2024_PAGE = f"{INSEE}/8582147"


def zip_csv(content: bytes, suffix: str = ".csv") -> str:
    """The one data CSV of a zip (not the `meta_` file)."""
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(suffix) and not n.lower().startswith("meta")]
        if len(names) != 1:
            raise ValueError(f"expected one CSV in the zip, found {names}")
        return z.read(names[0]).decode("utf-8-sig")


def _dep(code: str) -> str:
    return code.strip()


def img1b_rows(csv_text: str, year: str, prefix: str, column: str) -> list[dict]:
    """Population immigrée by département and country of birth: sexes and ages together. `prefix` tells V1 (A) from V2 (B)."""
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    need = {"NIVGEO", "CODGEO", "SEXE", "AGE4", column, "NB"}
    if need - set(reader.fieldnames or ()):
        raise ValueError(f"IMG1B: columns {sorted(need - set(reader.fieldnames or ()))} are gone")
    out = []
    for r in reader:
        if r["NIVGEO"] != "DEP" or r["SEXE"] != "ENS" or r["AGE4"] != "ENS" or r["NB"] in ("", None):
            continue
        out.append({"pays": f"{prefix}{r[column]}", "geo": _dep(r["CODGEO"]), "time": year, "value": float(r["NB"])})
    return out


def img1a_population(csv_text: str, year: str) -> list[dict]:
    """Population of each département at the census: the sum of its communes (IMG1A, immigrants and not, sexes and ages together)."""
    reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
    need = {"NIVGEO", "CODGEO", "NB"}
    if need - set(reader.fieldnames or ()):
        raise ValueError(f"IMG1A: columns {sorted(need - set(reader.fieldnames or ()))} are gone")
    total: dict[str, float] = {}
    for r in reader:
        if r["NIVGEO"] != "COM" or r["NB"] in ("", None):
            continue
        c = r["CODGEO"].strip()
        d = c[:3] if c.startswith("97") else c[:2]
        total[d] = total.get(d, 0.0) + float(r["NB"])
    return [{"pays": "POP", "geo": d, "time": year, "value": v} for d, v in total.items()]


def immigrants_rows(year: str, v1: bytes, v2: bytes, pop: bytes, min_deps: int = 90) -> list[dict]:
    rows = img1b_rows(zip_csv(v1), year, "A", "PNAI12_A") + img1b_rows(zip_csv(v2), year, "B", "PNAI48_A") + img1a_population(zip_csv(pop), year)
    if sum(1 for r in rows if r["pays"] == "AENS") < min_deps:
        raise ValueError(f"IMG1B {year}: fewer than {min_deps} départements")
    return rows


_DEP_CODE = re.compile(r"^(?:\d{2})(\d{2}|2A|2B|\d{3})$")


def births_2024_rows(csv_text: str) -> list[dict]:
    """N7DBIS 2024: births by the mother's country of birth (a selection) and département of residence.

    `REGDEP_DOMI_MERE` is the region code then the département code (1175 = region 11, Paris); regional totals end in XX;
    overseas départements are 971 to 976; FM and FR are metropolitan France and France, ENR the "registered in France" lines.
    """
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("﻿")), delimiter=";")
    if {"REGDEP_DOMI_MERE", "PNSEL_MERE", "NBNAIS"} - set(reader.fieldnames or ()):
        raise ValueError("N7DBIS: columns are gone")
    out = []
    for r in reader:
        code = r["REGDEP_DOMI_MERE"].strip()
        if code in ("FM", "FR"):
            geo = code
        elif re.fullmatch(r"97[1-6]", code):
            geo = code
        elif re.fullmatch(r"\d{2}(?:\d{2}|2A|2B)", code) and not code.endswith("XX"):
            geo = code[2:]
        else:
            continue
        try:
            value = float(r["NBNAIS"])
        except ValueError:
            continue
        out.append({"pays_mere": r["PNSEL_MERE"].strip(), "geo": geo, "time": "2024", "value": value})
    return out
