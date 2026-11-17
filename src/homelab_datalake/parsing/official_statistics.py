"""Official statistics on France and its EU neighbours: what is read, and how it is shaped.

politico.fraserv.fr's Données section charts these series. They are public figures from
public agencies, so nothing here is private; the lake keeps them for the same reason it keeps
everything else -- one place, with history. Eurostat and the SSMSI revise their figures, and
a ledger (parsing/ledger.py) keeps every version a row has had, so a revision is a new
version rather than a silent overwrite.

Two publishers originally (the public-spending inputs of parsing/public_spending.py came later):

* **Eurostat**, through its dissemination API (JSON-stat 2.0, no key). Each query names a
  dataset and narrows its dimensions to the codes a chart needs. Unknown codes are ignored by
  the API, so every query can ask for the EU27 countries and the EU27_2020 aggregate whether
  or not the dataset carries the aggregate.
* **The SSMSI** (the Interior Ministry's statistics service), whose departmental base of
  recorded crime is a CSV on data.gouv.fr. The newest release is found through the
  data.gouv.fr API, so a new edition is picked up without a code change. Department rows are
  kept, and the national total is added as geo `FR`.

Pure functions only; assets/official_statistics.py does the fetching and writing.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from urllib.parse import urlencode

EUROSTAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
EUROSTAT_VIEW = "https://ec.europa.eu/eurostat/databrowser/view/{code}/default/table?lang=fr"
SSMSI_DATASET_ID = "621df2954fa5a3b5a023e23c"
SSMSI_DATASET_API = f"https://www.data.gouv.fr/api/1/datasets/{SSMSI_DATASET_ID}/"
SSMSI_PAGE = f"https://www.data.gouv.fr/datasets/{SSMSI_DATASET_ID}"
SSMSI_CODE = "delinquance_departementale"
SSMSI_DEP_CODE = "delinquance_dep_taux"

EU27 = ("AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "EL", "ES", "FI", "FR", "HR", "HU",
        "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK")
GEOS = (*EU27, "EU27_2020")
CTZ_GEOS = ("FR", "EU27_2020")  # the by-citizenship queries: France and the EU as a whole

KEY_COLUMNS = ("source", "dataset", "series_key", "geo", "period")
# Metadata that moves without the figure moving: a dataset's "last updated" stamp changes
# with every Eurostat release even for rows nobody revised.
VOLATILE_COLUMNS = frozenset({"source_updated", "dataset_label", "source_url"})
LEDGER_COLUMNS = (*KEY_COLUMNS, "value", "status", "unit", "dims_json",
                  "dataset_label", "source_updated", "source_url")


@dataclass(frozen=True)
class EurostatQuery:
    code: str
    filters: dict[str, tuple[str, ...]]
    since: str
    why: str
    geos: tuple[str, ...] = field(default=GEOS)
    # Two queries on the same dataset (different filters) need distinct keys in the ledger
    # and the export; the alias is that key. The publisher's code is still what is requested.
    alias: str = ""

    @property
    def key(self) -> str:
        return self.alias or self.code

    def url(self) -> str:
        params: list[tuple[str, str]] = [("format", "JSON"), ("lang", "fr"),
                                         ("sinceTimePeriod", self.since)]
        for dim, codes in self.filters.items():
            params.extend((dim, code) for code in codes)
        params.extend(("geo", g) for g in self.geos)
        return f"{EUROSTAT_API}{self.code}?{urlencode(params)}"


def _q(code, since, why, *, alias="", geos=GEOS, **filters):
    return EurostatQuery(code, {k: tuple(v) if isinstance(v, (list, tuple)) else (v,) for k, v in filters.items()},
                         since, why, geos=geos, alias=alias)


EUROSTAT_QUERIES: tuple[EurostatQuery, ...] = (
    # immigration
    _q("migr_resfirst", "2008", "First residence permits by reason",
       reason=["TOTAL", "FAM", "EDUC", "EMP", "OTH"], duration="TOTAL", citizen="TOTAL", unit="PER"),
    _q("migr_eiord", "2010", "Third-country nationals ordered to leave",
       citizen="TOTAL", age="TOTAL", sex="T", unit="PER"),
    _q("migr_eirtn", "2010", "Third-country nationals who left after an order to leave",
       citizen="TOTAL", c_dest=["TOTAL", "THRD"], age="TOTAL", sex="T", unit="PER"),
    _q("migr_asyappctza", "2008", "First-time asylum applicants",
       citizen="TOTAL", applicant="FRST", sex="T", age="TOTAL", unit="PER"),
    _q("migr_acq", "2008", "Acquisitions of citizenship",
       citizen="TOTAL", agedef="COMPLET", age="TOTAL", sex="T", unit="NR"),
    _q("migr_pop1ctz", "2007", "Population by citizenship (foreigners = total minus nationals)",
       citizen=["TOTAL", "NAT"], age="TOTAL", sex="T", unit="NR"),
    # The same four datasets broken down by citizenship (every citizenship Eurostat publishes), for France and the EU
    # as a whole: the "where do they come from" maps. The queries above stay as they are, restricted to the total.
    _q("migr_resfirst", "2008", "First residence permits by citizenship (all citizenships), France and EU27",
       alias="migr_resfirst_ctz", geos=CTZ_GEOS, reason="TOTAL", duration="TOTAL", unit="PER"),
    _q("migr_asyappctza", "2008", "First-time asylum applicants by citizenship (all citizenships), France and EU27",
       alias="migr_asyappctza_ctz", geos=CTZ_GEOS, applicant="FRST", sex="T", age="TOTAL", unit="PER"),
    _q("migr_acq", "2008", "Acquisitions of citizenship by previous citizenship (all citizenships), France and EU27",
       alias="migr_acq_ctz", geos=CTZ_GEOS, agedef="COMPLET", age="TOTAL", sex="T", unit="NR"),
    _q("migr_pop1ctz", "2007", "Population on 1 January by citizenship (every citizenship published), France and EU27",
       alias="migr_pop1ctz_ctz", geos=CTZ_GEOS, age="TOTAL", sex="T", unit="NR"),
    _q("lfsa_ergacob", "2015", "Employment rate of 20-64 year-olds by country of birth",
       sex="T", age="Y20-64", c_birth=["NAT", "FOR", "NEU27_2020_FOR"], unit="PC"),
    _q("lfsa_urgacob", "2015", "Unemployment rate of 15-74 year-olds by country of birth",
       sex="T", age="Y15-74", c_birth=["NAT", "FOR", "NEU27_2020_FOR"], unit="PC"),
    _q("ilc_li32", "2015", "At-risk-of-poverty rate of adults by country of birth",
       age="Y_GE18", sex="T", unit="PC", c_birth=["NAT", "FOR", "NEU27_2020_FOR"]),
    _q("demo_facbc", "2007", "Live births by the mother's country of birth",
       age="TOTAL", unit="NR", c_birth=["TOTAL", "NAT", "FOR", "NEU27_2020_FOR", "EU27_2020_FOR"]),
    # security
    _q("crim_pris_ctz", "2015", "Prisoners by citizenship",
       citizen=["TOTAL", "NAT", "FOR"], unit=["NR", "P_HTHAB"]),
    _q("crim_pris_cap", "2015", "Prison capacity and persons held",
       indic_cr=["PRIS_OFF_CAP", "PRIS_ACT_CAP"], unit="NR"),
    _q("crim_off_cat", "2008", "Intentional homicides recorded by the police",
       iccs="ICCS0101", unit=["NR", "P_HTHAB"]),
    # public finances
    _q("irt_lt_mcby_m", "2015-01", "Ten-year government bond yields (Maastricht criterion), monthly",
       int_rt="MCBY"),
    _q("gov_10a_main", "1995", "General government interest payable and total expenditure",
       na_item=["D41PAY", "TE"], unit=["MIO_EUR", "PC_GDP"], sector="S13"),
    _q("gov_10a_exp", "2015", "General government expenditure by function: defence, social protection, old age",
       cofog99=["TOTAL", "GF02", "GF10", "GF1002"], unit=["MIO_EUR", "PC_GDP"], sector="S13", na_item="TE"),
    _q("gov_10a_taxag", "1995", "Total receipts from taxes and social contributions",
       na_item="D2_D5_D91_D61_M_D995", unit="PC_GDP", sector="S13_S212"),
    _q("gov_10q_ggdebt", "2000-Q1", "Quarterly general government gross debt",
       na_item="GD", sector="S13", unit=["MIO_EUR", "PC_GDP"]),
    _q("gov_10dd_edpt1", "1995", "Government deficit and debt",
       na_item=["B9", "GD"], unit=["PC_GDP", "MIO_EUR"], sector="S13"),
    # economy
    _q("prc_ppp_ind", "1995", "GDP and actual individual consumption per head in PPS, EU27 = 100",
       na_item="VI_PPS_EU27_2020_HAB", ppp_cat=["GDP", "A01"]),
    _q("nama_10_lp_ulc", "1995", "Real labour productivity per hour worked",
       na_item="RLPR_HW", unit="I15"),
    _q("nama_10_a10", "1995", "Manufacturing share of gross value added",
       na_item="B1G", unit="PC_TOT", nace_r2="C"),
    _q("nama_10_gdp", "1995", "Exports and imports of goods, GDP",
       na_item=["P61", "P71", "B1GQ"], unit="CP_MEUR"),
    # demography
    _q("demo_gind", "1994", "Births, deaths, natural change, net migration, population",
       indic_de=["LBIRTH", "DEATH", "NATGROW", "CNMIGRAT", "GROW", "JAN"]),
    _q("demo_find", "2000", "Total fertility rate", indic_de="TOTFERRT"),
    _q("demo_pjanind", "1990", "Old-age dependency ratio", indic_de="OLDDEP1"),
    # school
    _q("sdg_04_40", "2000", "Low-achieving 15-year-olds in PISA",
       levels="LOW", field=["READ", "EF461", "SCI"], sex="T", unit="PC"),
    # public spending (Données > Dépenses publiques): France only, every function and sub-function
    _q("gov_10a_exp", "1995", "French general government expenditure by function (COFOG, all 80 codes), "
       "total / compensation of employees / social benefits, by sub-sector",
       alias="gov_10a_exp_fr", geos=("FR",), sector=["S13", "S1311", "S1313", "S1314"],
       na_item=["TE", "D1", "D62"], unit=["MIO_EUR", "PC_GDP"]),
    _q("gov_10a_taxag", "1995", "Social contributions received by sub-sector, actual and imputed employers' ones",
       alias="gov_10a_taxag_fr", geos=("FR",), sector=["S13", "S1311", "S1313", "S1314"],
       na_item=["D61", "D611", "D612", "D613"], unit="MIO_EUR"),
    _q("nasa_10_nf_tr", "1995", "Compensation of employees, employers' social contributions and benefits paid by the "
       "government as a whole (the dataset has no sub-sectors)", alias="nasa_10_nf_tr_fr", geos=("FR",), sector="S13",
       direct="PAID", na_item=["D1", "D12", "D62"], unit="CP_MEUR"),
    _q("nama_10_gdp", "1995", "GDP in chain-linked volumes (2020 prices), to deflate current-price figures",
       alias="nama_10_gdp_vol", geos=("FR",), na_item="B1GQ", unit="CLV20_MEUR"),
    _q("nama_10_gdp", "1995", "Compensation of employees of the whole economy (income approach): the labour cost social contributions are measured against",
       alias="nama_10_gdp_d1", geos=("FR",), na_item="D1", unit="CP_MEUR"),
    _q("gov_10a_main", "1995", "General government total revenue and interest payable, EU comparison of the interest burden",
       alias="gov_10a_main_tr", na_item=["TR", "D41PAY"], unit=["MIO_EUR", "PC_GDP"], sector="S13"),
    _q("irt_lt_mcby_a", "1990", "Ten-year government bond yields (Maastricht criterion), annual", int_rt="MCBY"),
    _q("nama_10_pe", "1995", "Total employment (domestic concept), thousand persons", geos=("FR",), na_item="EMP_DC", unit="THS_PER"),
    _q("earn_nt_taxwedge", "2001", "Tax wedge on labour costs of a low-wage earner (67 % of the average wage), EU comparison"),
)


def eurostat_view_url(code: str) -> str:
    return EUROSTAT_VIEW.format(code=code)


# ---------------------------------------------------------------- JSON-stat

def jsonstat_rows(payload: dict) -> tuple[list[dict], dict[str, dict[str, str]]]:
    """Flatten a JSON-stat 2.0 dataset into one dict per value, plus every category label.

    `value` and `status` may each be a list or a sparse object keyed by the flat index;
    Eurostat sends objects. A missing value is simply absent from the result.
    """
    ids: list[str] = payload["id"]
    sizes: list[int] = payload["size"]
    dims = payload["dimension"]
    codes = [sorted(dims[d]["category"]["index"].items(), key=lambda kv: kv[1]) for d in ids]
    labels = {d: dict(dims[d]["category"].get("label", {})) for d in ids}
    values = payload.get("value", {})
    statuses = payload.get("status", {}) or {}
    items = values.items() if isinstance(values, dict) else enumerate(values)
    out: list[dict] = []
    for flat, value in items:
        if value is None:
            continue
        flat = int(flat)
        rest, coords = flat, []
        for k in reversed(range(len(ids))):
            coords.append(codes[k][rest % sizes[k]][0])
            rest //= sizes[k]
        coords.reverse()
        status = statuses.get(str(flat)) if isinstance(statuses, dict) else (
            statuses[flat] if flat < len(statuses) else None)
        out.append({**dict(zip(ids, coords, strict=True)), "value": float(value), "status": status})
    return out, labels


def series_key(dims: dict[str, str]) -> str:
    """The non-geo, non-time coordinates of a value, as `dim=code;dim=code` in id order."""
    return ";".join(f"{k}={v}" for k, v in dims.items())


def eurostat_ledger_rows(query: EurostatQuery, payload: dict) -> list[dict]:
    rows, _labels = jsonstat_rows(payload)
    updated = payload.get("updated")
    label = payload.get("label")
    out = []
    for row in rows:
        dims = {k: v for k, v in row.items() if k not in ("geo", "time", "value", "status", "freq")}
        out.append({
            "source": "eurostat", "dataset": query.key, "series_key": series_key(dims),
            "geo": row.get("geo"), "period": row.get("time"), "value": row["value"],
            "status": row.get("status"), "unit": dims.get("unit"),
            "dims_json": json.dumps(dims, sort_keys=True), "dataset_label": label,
            "source_updated": updated, "source_url": eurostat_view_url(query.code),
        })
    return out


def eurostat_block(query: EurostatQuery, payload: dict, fetched_at: str) -> dict:
    """The export block for one Eurostat query: compact rows plus the labels a chart shows."""
    rows, labels = jsonstat_rows(payload)
    dims = [d for d in payload["id"] if d not in ("freq",)]
    order = [d for d in dims if d not in ("geo", "time")] + ["geo", "time"]
    return {
        "source": "eurostat", "code": query.key, "label": payload.get("label"),
        "updated": payload.get("updated"), "url": eurostat_view_url(query.code),
        "fetched_at": fetched_at, "status": "ok", "dims": order,
        "labels": {d: labels.get(d, {}) for d in order if d != "time"},
        "rows": [[r.get(d) for d in order] + [r["value"]] for r in rows],
    }


# ---------------------------------------------------------------- SSMSI

def ssmsi_resource(dataset: dict) -> dict:
    """The departmental CSV among the data.gouv.fr dataset's resources, newest first."""
    candidates = [r for r in dataset.get("resources", [])
                  if (r.get("format") or "").lower() == "csv" and (r.get("title") or "").startswith("DEP ")]
    if not candidates:
        raise ValueError("the SSMSI dataset no longer lists a departmental CSV (title 'DEP - ...')")
    return max(candidates, key=lambda r: r.get("last_modified") or "")


def _number(text: str) -> float | None:
    text = (text or "").strip().replace("\u202f", "").replace("\u00a0", "").replace(" ", "").replace(",", ".")
    if not text or text.upper() == "NA":
        return None
    return float(text)


def ssmsi_rows(csv_text: str) -> list[dict]:
    """Department rows, plus the national total per indicator and year as geo `FR`.

    Columns as published: Code_departement; Code_region; annee; indicateur; unite_de_compte;
    nombre; taux_pour_mille; insee_pop; ... A `nombre` the SSMSI suppressed is absent and
    counts as nothing in the total -- the national figures it publishes are built the same way.
    """
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("﻿")), delimiter=";")
    required = {"Code_departement", "annee", "indicateur", "unite_de_compte", "nombre"}
    missing = required - set(reader.fieldnames or ())
    if missing:
        raise ValueError(f"SSMSI CSV is missing columns {sorted(missing)}")
    out: list[dict] = []
    totals: dict[tuple[str, str, str], float] = {}
    for row in reader:
        value = _number(row["nombre"])
        if value is None:
            continue
        indicator, unit, year = row["indicateur"].strip(), row["unite_de_compte"].strip(), row["annee"].strip()
        out.append({"indicateur": indicator, "unite_de_compte": unit,
                    "geo": f"FR-D{row['Code_departement'].strip()}", "time": year, "value": value})
        key = (indicator, unit, year)
        totals[key] = totals.get(key, 0.0) + value
    out.extend({"indicateur": i, "unite_de_compte": u, "geo": "FR", "time": y, "value": v}
               for (i, u, y), v in totals.items())
    return out


def ssmsi_ledger_rows(rows: list[dict], resource: dict) -> list[dict]:
    label = resource.get("title")
    return [{
        "source": "ssmsi", "dataset": SSMSI_CODE,
        "series_key": series_key({"indicateur": r["indicateur"], "unite_de_compte": r["unite_de_compte"]}),
        "geo": r["geo"], "period": r["time"], "value": r["value"], "status": None,
        "unit": r["unite_de_compte"],
        "dims_json": json.dumps({"indicateur": r["indicateur"], "unite_de_compte": r["unite_de_compte"]},
                                sort_keys=True, ensure_ascii=False),
        "dataset_label": label, "source_updated": resource.get("last_modified"), "source_url": SSMSI_PAGE,
    } for r in rows]


def ssmsi_block(rows: list[dict], resource: dict, fetched_at: str) -> dict:
    national = [r for r in rows if r["geo"] == "FR"]
    return {
        "source": "ssmsi", "code": SSMSI_CODE, "label": resource.get("title"),
        "updated": resource.get("last_modified"), "url": SSMSI_PAGE, "file": resource.get("url"),
        "fetched_at": fetched_at, "status": "ok", "dims": ["indicateur", "unite_de_compte", "geo", "time"],
        "labels": {"geo": {"FR": "France"}},
        "rows": sorted([[r["indicateur"], r["unite_de_compte"], "FR", r["time"], r["value"]] for r in national]),
    }


def ssmsi_dep_rows(csv_text: str) -> list[dict]:
    """Every département's recorded crime with the rate the SSMSI publishes and its denominators.

    One row per (indicator, unit of count, measure, département, year): `nombre`, `taux_pour_mille`
    (per 1 000 inhabitants, except burglaries of dwellings, which the SSMSI rates per 1 000 dwellings),
    plus the population and the number of dwellings (`population`, `logements`) once per département and
    year, filed under the pseudo-indicator `_denominateur`. A missing figure (`NA`) is simply absent.
    """
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")), delimiter=";")
    need = {"Code_departement", "annee", "indicateur", "unite_de_compte", "nombre", "taux_pour_mille", "insee_pop", "insee_log"}
    if need - set(reader.fieldnames or ()):
        raise ValueError(f"SSMSI departmental file: columns {sorted(need - set(reader.fieldnames or ()))} are gone")
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for r in reader:
        geo, year = r["Code_departement"].strip(), r["annee"].strip()
        indicator, unit = r["indicateur"].strip(), r["unite_de_compte"].strip()
        for measure, key in (("nombre", "nombre"), ("taux_pour_mille", "taux_pour_mille")):
            value = _number(r[key])
            if value is not None:
                out.append({"indicateur": indicator, "unite": unit, "mesure": measure, "geo": geo, "time": year, "value": value})
        if (geo, year) not in seen:
            seen.add((geo, year))
            for measure, key in (("population", "insee_pop"), ("logements", "insee_log")):
                value = _number(r[key])
                if value is not None:
                    out.append({"indicateur": "_denominateur", "unite": "", "mesure": measure, "geo": geo, "time": year, "value": value})
    return out


def ssmsi_dep_block(rows: list[dict], resource: dict, fetched_at: str) -> dict:
    return {
        "source": "ssmsi", "code": SSMSI_DEP_CODE,
        "label": f"{resource.get('title')} : taux pour 1 000 habitants par département",
        "updated": resource.get("last_modified"), "url": SSMSI_PAGE, "file": resource.get("url"),
        "fetched_at": fetched_at, "status": "ok", "dims": ["indicateur", "unite", "mesure", "geo", "time"], "labels": {},
        "rows": sorted([[r["indicateur"], r["unite"], r["mesure"], r["geo"], r["time"], r["value"]] for r in rows]),
    }


def ssmsi_dep_ledger_rows(rows: list[dict], resource: dict) -> list[dict]:
    """Rates and denominators go to the ledger (the counts already do, as `delinquance_departementale`)."""
    keep = [r for r in rows if r["mesure"] != "nombre"]
    return [{
        "source": "ssmsi", "dataset": SSMSI_DEP_CODE,
        "series_key": series_key({"indicateur": r["indicateur"], "unite_de_compte": r["unite"], "mesure": r["mesure"]}),
        "geo": f"FR-D{r['geo']}", "period": r["time"], "value": r["value"], "status": None, "unit": r["unite"],
        "dims_json": json.dumps({"indicateur": r["indicateur"], "unite_de_compte": r["unite"], "mesure": r["mesure"]},
                                sort_keys=True, ensure_ascii=False),
        "dataset_label": resource.get("title"), "source_updated": resource.get("last_modified"), "source_url": SSMSI_PAGE,
    } for r in keep]


# ---------------------------------------------------------------- export

EXPORT_FORMAT = 1


def build_export(blocks: dict[str, dict], previous: dict | None, generated_at: str) -> dict:
    """Today's blocks, with yesterday's kept (marked stale) for any source that failed today.

    A publisher that is down for a night must not empty the site's charts; the stale flag
    lets the site say how old a figure is instead of pretending it is fresh.
    """
    datasets = dict(blocks)
    for key, block in ((previous or {}).get("datasets") or {}).items():
        if key not in datasets:
            datasets[key] = {**block, "status": "stale"}
    return {"format": EXPORT_FORMAT, "generated_at": generated_at, "datasets": datasets}


def dataset_key(source: str, code: str) -> str:
    return f"{source}:{code}"
