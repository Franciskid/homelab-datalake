"""Indicators by département that politico.fraserv.fr's « Croiser deux chiffres » needs and that no other block holds (Insee, Licence Ouverte).

* **Filosofi 2023** (revenus et pauvreté des ménages, tous niveaux géographiques): median standard of living, poverty rate (60 % of the median),
  the ratio of the 9th to the 1st decile, the share of social benefits in disposable income.
* **Recensement 2023, base des principaux indicateurs** (new "DS_" format, long tables with every geography): the share of people aged 15 or more
  with no diploma or a primary certificate and the share with a higher-education diploma (not at school); the unemployment rate of people aged
  15 to 64 as the census counts it; the share of main homes that are overcrowded (moderately or severely).

Each indicator: { label, definition, unit, year, direction, values: { département code: number } }. Pure functions; assets/department_indicators.py
fetches the zips and writes `departements.json` next to the export.
"""

from __future__ import annotations

import csv
import io
import zipfile

INSEE = "https://www.insee.fr/fr/statistiques"
SOURCES = {
    "filosofi": {"label": "Insee, Filosofi 2023", "title": "Revenus et pauvreté des ménages en 2023", "page": f"{INSEE}/8984752",
                 "zip": f"{INSEE}/fichier/8984752/FILOSOFI_CC_csv.zip"},
    "diplomes": {"label": "Insee, recensement de la population 2023", "title": "Diplômes-Formation en 2023 (base des principaux indicateurs)", "page": f"{INSEE}/9006527",
                 "zip": f"{INSEE}/fichier/9006527/DS_RP_DIPLOMES_PRINC_2023_CSV_FR.zip"},
    "emploi": {"label": "Insee, recensement de la population 2023", "title": "Emploi-Population active en 2023 (base des principaux indicateurs)", "page": f"{INSEE}/9002680",
               "zip": f"{INSEE}/fichier/9002680/DS_RP_EMPLOI_LR_PRINC_2023_CSV_FR.zip"},
    "logement": {"label": "Insee, recensement de la population 2023", "title": "Logement en 2023 (base des principaux indicateurs)", "page": f"{INSEE}/8997194",
                 "zip": f"{INSEE}/fichier/8997194/DS_RP_LOGEMENT_COMP_2023_CSV_FR.zip"},
}


def data_csv(content: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if n.lower().endswith("_data.csv")]
        if len(names) != 1:
            raise ValueError(f"expected one data CSV in the zip, found {z.namelist()[:5]}")
        return z.read(names[0]).decode("utf-8-sig")


def dep_cells(text: str, year: str, keep: dict[str, set[str]], value: str = "OBS_VALUE"):
    """The département rows of a long table for `year` whose dimensions are in `keep` ({column: allowed codes}):
    yields (département code, row)."""
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    need = {"GEO", "GEO_OBJECT", "TIME_PERIOD", value, *keep}
    if need - set(reader.fieldnames or ()):
        raise ValueError(f"columns {sorted(need - set(reader.fieldnames or ()))} are gone")
    for r in reader:
        if r["GEO_OBJECT"] != "DEP" or r["TIME_PERIOD"] != year or r[value] in ("", None):
            continue
        if all(r[c] in allowed for c, allowed in keep.items()):
            yield r["GEO"].strip(), r


def _share(num: dict[str, float], den: dict[str, float], scale: float = 100.0) -> dict[str, float]:
    return {d: round(scale * num[d] / den[d], 3) for d in num if den.get(d)}


def filosofi(content: bytes) -> dict[str, dict]:
    text = data_csv(content)
    measures = {"MED_SL": "median", "PR_MD60": "poverty", "IR_D9_D1_SL": "ratio", "S_SOC_BEN_DI": "benefits"}
    out: dict[str, dict[str, float]] = {k: {} for k in measures.values()}
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    for r in reader:
        if r["GEO_OBJECT"] == "DEP" and r["FILOSOFI_MEASURE"] in measures and r["OBS_VALUE"] not in ("", None):
            out[measures[r["FILOSOFI_MEASURE"]]][r["GEO"].strip()] = float(r["OBS_VALUE"])
    return {
        "revenu_median": {"values": out["median"], "year": "2023", "unit": "€ par an et par unité de consommation", "direction": "higher_is_more",
                          "label": "Niveau de vie médian", "definition": "Revenu disponible par unité de consommation qui partage les personnes en deux moitiés égales (Filosofi, hors Mayotte)."},
        "pauvrete": {"values": out["poverty"], "year": "2023", "unit": "%", "direction": "higher_is_more",
                     "label": "Taux de pauvreté monétaire", "definition": "Part des personnes dont le niveau de vie est inférieur à 60 % du niveau de vie médian de la France (seuil national)."},
        "inegalites_d9_d1": {"values": out["ratio"], "year": "2023", "unit": "rapport", "direction": "higher_is_more",
                             "label": "Inégalités de niveau de vie (D9/D1)", "definition": "Rapport entre le niveau de vie plancher des 10 % les plus aisés et le niveau de vie plafond des 10 % les plus modestes du département."},
        "prestations_sociales": {"values": out["benefits"], "year": "2023", "unit": "% du revenu disponible", "direction": "higher_is_more",
                                 "label": "Part des prestations sociales dans le revenu disponible", "definition": "Prestations sociales (allocations familiales, logement, minima sociaux…) en part du revenu disponible des ménages."},
    }


def diplomes(content: bytes) -> dict[str, dict]:
    text = data_csv(content)
    base = {"AGE": {"Y_GE15"}, "SEX": {"_T"}, "RP_MEASURE": {"POP"}}
    tot, low, high = {}, {}, {}
    for dep, r in dep_cells(text, "2023", {**base, "EDUC": {"_T", "001T100_RP", "500T702_RP"}}):
        {"_T": tot, "001T100_RP": low, "500T702_RP": high}[r["EDUC"]][dep] = float(r["OBS_VALUE"])
    return {
        "sans_diplome": {"values": _share(low, tot), "year": "2023", "unit": "%", "direction": "higher_is_more",
                         "label": "Part des 15 ans ou plus sans diplôme ou avec le certificat d’études", "definition": "Personnes de 15 ans ou plus non scolarisées sans diplôme ou avec le seul certificat d’études primaires, en part de ces personnes (recensement)."},
        "diplome_superieur": {"values": _share(high, tot), "year": "2023", "unit": "%", "direction": "higher_is_more",
                              "label": "Part des 15 ans ou plus diplômés du supérieur", "definition": "Personnes de 15 ans ou plus non scolarisées titulaires d’un diplôme de l’enseignement supérieur (bac+2 ou plus), en part de ces personnes (recensement)."},
    }


def emploi(content: bytes) -> dict[str, dict]:
    text = data_csv(content)
    active, unemp = {}, {}
    for dep, r in dep_cells(text, "2023", {"SEX": {"_T"}, "AGE": {"Y15T64"}, "EDUC": {"_T"}, "RP_MEASURE": {"POP"}, "EMPSTA_ENQ": {"1T2", "2"}}):
        (active if r["EMPSTA_ENQ"] == "1T2" else unemp)[dep] = float(r["OBS_VALUE"])
    return {"chomage_rp": {"values": _share(unemp, active), "year": "2023", "unit": "%", "direction": "higher_is_more",
                           "label": "Taux de chômage (recensement)", "definition": "Chômeurs au sens du recensement parmi les actifs de 15 à 64 ans : une mesure déclarative, plus élevée que le chômage au sens du BIT."}}


def logement(content: bytes) -> dict[str, dict]:
    text = data_csv(content)
    tot, over = {}, {}
    for dep, r in dep_cells(text, "2023", {"OCS": {"DW_MAIN"}, "RP_MEASURE": {"DWELLINGS"}, "OCC_IND": {"_T", "MOD_OVER_OCC", "SEV_OVER_OCC"}}):
        if r["OCC_IND"] == "_T":
            tot[dep] = float(r["OBS_VALUE"])
        else:
            over[dep] = over.get(dep, 0.0) + float(r["OBS_VALUE"])
    return {"suroccupation": {"values": _share(over, tot), "year": "2023", "unit": "%", "direction": "higher_is_more",
                              "label": "Part des résidences principales en suroccupation", "definition": "Résidences principales où il manque au moins une pièce par rapport à la norme de peuplement de l’Insee (suroccupation modérée ou accentuée)."}}


PARSERS = {"filosofi": filosofi, "diplomes": diplomes, "emploi": emploi, "logement": logement}
