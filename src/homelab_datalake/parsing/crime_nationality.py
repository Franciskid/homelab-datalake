"""Offending by nationality, with the population of the same nationality: nine official sources, one comparable shape.

Politico's Données section compares *rates*: people of a nationality who were charged or convicted in a year, divided by the residents of
that nationality, nationals being the base. Every figure below comes from the publisher's own open table; none is estimated here.

* **Switzerland, OFS**: the Strafurteilsstatistik (SUS) joined to STATPOP, one SDMX table (`DF_SUS_NATIONALITIES_LAWS`): persons convicted
  *and* the permanent resident population by nationality (178), sex, age band and law, 2024. And `DF_SUS_PERSONS`: persons convicted by
  citizenship group (Swiss, foreign residents, foreign non-residents) and sex, 2007-2025.
* **Germany, BKA**: the PKS table T62, suspects by offence and by nationality (every nationality with at least one suspect), 2024 and 2025.
* **Netherlands, CBS**: registrations of suspects by offence, sex, nationality and residence (85656NED), and the population by nationality
  (85510NED).
* **Denmark, Statistics Denmark**: persons found guilty by sex, age band and country of origin (STRAFNA3) and the population by the same
  (FOLK2, 15-79 years). Denmark counts *origin* (immigrants and their descendants), not nationality.
* **Italy, ISTAT**: alleged offenders reported by the police by offence, sex, age and Italian or foreign citizenship (73_230).
* **Spain, INE**: adults convicted by sex, age and nationality group (CONA 25645), convictions for domestic and gender violence by place of
  birth (VGD 28402, 28305).
* **France, SSMSI**: people charged for offences cleared up, by sex, age and French or foreign nationality (data.gouv.fr).
* **Eurostat migr_pop1ctz**: the population by citizenship, for Germany, Denmark, Spain, France, Italy, the Netherlands and Switzerland.

Pure functions only; assets/crime_nationality.py fetches.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata

from .country_names import COUNTRIES
from .official_statistics import jsonstat_rows
from .xlsx import read as read_xlsx

SOURCE = "crime"

# ---------------------------------------------------------------- countries


def norm(text: str) -> str:
    """Lower case, no accents, letters and digits only: how publishers' country names are compared."""
    text = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


_BY_NAME: dict[str, str] = {}
_BY_ISO2: dict[str, str] = {}
for _iso3, _iso2, _names in COUNTRIES:
    if _iso2:
        _BY_ISO2.setdefault(_iso2, _iso3)
    for _name in _names.values():
        if _name:
            _BY_NAME.setdefault(norm(_name), _iso3)

# Names the publishers use that Natural Earth spells differently (normalised on the left).
ALIASES: dict[str, str] = {
    "turkiye": "TUR", "turkey": "TUR", "turkei": "TUR", "turkije": "TUR", "turquie": "TUR",
    "russian federation": "RUS", "russische foderation": "RUS", "russland": "RUS", "russia": "RUS",
    "korea south": "KOR", "south korea": "KOR", "republic of korea": "KOR", "korea republik": "KOR", "korea, republic of": "KOR",
    "korea north": "PRK", "north korea": "PRK", "korea demokratische volksrepublik": "PRK",
    "usa": "USA", "united states of america": "USA", "vereinigte staaten": "USA", "vereinigte staaten von amerika": "USA",
    "united kingdom": "GBR", "vereinigtes konigreich": "GBR", "great britain": "GBR",
    "czechia": "CZE", "czech republic": "CZE", "tschechien": "CZE", "tschechische republik": "CZE",
    "bosnia and herzegovina": "BIH", "bosnien und herzegowina": "BIH", "bosnia herzegovina": "BIH",
    "north macedonia": "MKD", "republic of north macedonia": "MKD", "nordmazedonien": "MKD", "macedonia": "MKD", "mazedonien": "MKD",
    "moldova": "MDA", "moldova republic of": "MDA", "moldau": "MDA", "republik moldau": "MDA",
    "syria": "SYR", "syrien": "SYR", "syrian arab republic": "SYR", "viet nam": "VNM", "vietnam": "VNM",
    "iran": "IRN", "iran islamic republic of": "IRN", "laos": "LAO", "lao people s democratic republic": "LAO",
    "cote d ivoire": "CIV", "elfenbeinkuste": "CIV", "ivory coast": "CIV",
    "congo": "COG", "congo republic of": "COG", "republic of the congo": "COG", "kongo": "COG",
    "democratic republic of the congo": "COD", "congo democratic republic of": "COD", "kongo demokratische republik": "COD",
    "cabo verde": "CPV", "cape verde": "CPV", "kap verde": "CPV", "eswatini": "SWZ", "swaziland": "SWZ",
    "myanmar": "MMR", "burma": "MMR", "brunei": "BRN", "brunei darussalam": "BRN", "timor leste": "TLS", "east timor": "TLS",
    "palestine": "PSE", "palestinian territory": "PSE", "state of palestine": "PSE", "palastina": "PSE",
    "kosovo": "KOS", "tanzania": "TZA", "tanzania united republic of": "TZA", "vatican": "VAT", "holy see": "VAT",
    "serbia": "SRB", "serbien": "SRB", "montenegro": "MNE", "slovakia": "SVK", "slowakei": "SVK",
    "libya": "LBY", "libyen": "LBY", "venezuela": "VEN", "bolivia": "BOL", "micronesia": "FSM",
    "gambia": "GMB", "the gambia": "GMB", "bahamas": "BHS", "taiwan": "TWN", "hong kong": "HKG", "china": "CHN",
    "united arab emirates": "ARE", "vereinigte arabische emirate": "ARE", "saudi arabia": "SAU", "saudi arabien": "SAU",
    "belarus": "BLR", "weissrussland": "BLR", "wit rusland": "BLR", "netherlands": "NLD", "niederlande": "NLD",
    "ireland": "IRL", "irland": "IRL", "switzerland": "CHE", "schweiz": "CHE", "swaziland eswatini": "SWZ",
    "sao tome and principe": "STP", "sao tome und principe": "STP", "saint lucia": "LCA", "st lucia": "LCA",
    "dominican republic": "DOM", "dominikanische republik": "DOM", "central african republic": "CAF", "zentralafrikanische republik": "CAF",
    "equatorial guinea": "GNQ", "aquatorialguinea": "GNQ", "guinea bissau": "GNB", "papua new guinea": "PNG",
    "antigua and barbuda": "ATG", "trinidad and tobago": "TTO", "saint vincent and the grenadines": "VCT", "st vincent and the grenadines": "VCT",
    "saint kitts and nevis": "KNA", "st kitts and nevis": "KNA", "sri lanka": "LKA", "new zealand": "NZL",
    "south africa": "ZAF", "sudan": "SDN", "south sudan": "SSD", "iceland": "ISL", "luxembourg": "LUX",
    "yugoslavia": "YUG", "yugoslavia federal republic": "YUG", "serbia and montenegro": "YUG", "czechoslovakia": "CSK", "soviet union": "SUN",
    "united states": "USA", "taiwan chinese taipei": "TWN", "ussr": "SUN", "jugoslawien": "YUG", "serbien und montenegro": "YUG", "tschechoslowakei": "CSK", "sowjetunion": "SUN", "udssr": "SUN",
}

# Codes that stand for no single country, kept as they are.
STATELESS, UNKNOWN, OTHERS = "_STATELESS", "_UNKNOWN", "_OTHER"
TOTAL = "_T"


def iso3(name: str | None) -> str | None:
    """ISO3 of a country named in any of the publishers' languages, or None."""
    if not name:
        return None
    key = norm(name)
    return ALIASES.get(key) or _BY_NAME.get(key)


def iso3_from_iso2(code: str | None) -> str | None:
    if not code:
        return None
    code = code.strip().upper()
    return {"EL": "GRC", "UK": "GBR", "XK": "KOS"}.get(code) or _BY_ISO2.get(code)


# ---------------------------------------------------------------- blocks

def block(code: str, label: str, url: str, dims: list[str], rows: list[list], fetched_at: str, *,
          updated: str | None = None, extra: dict | None = None) -> dict:
    """An export block, in the shape of every other dataset: dims, then one row per value with the value last."""
    return {"source": SOURCE, "code": code, "label": label, "updated": updated, "url": url, "fetched_at": fetched_at,
            "status": "ok", "dims": dims, "labels": {}, "rows": rows, **(extra or {})}


def ledger(code: str, rows: list[dict], dims: list[str], url: str, label: str, updated: str | None) -> list[dict]:
    import json
    out = []
    for r in rows:
        coords = {d: r[d] for d in dims if d != "time"}
        out.append({"source": SOURCE, "dataset": code, "series_key": ";".join(f"{k}={v}" for k, v in coords.items()), "geo": "XX",
                    "period": str(r["time"]), "value": r["value"], "status": None, "unit": None,
                    "dims_json": json.dumps(coords, sort_keys=True, ensure_ascii=False),
                    "dataset_label": label, "source_updated": updated, "source_url": url})
    return out


# ---------------------------------------------------------------- Switzerland, OFS (SDMX)

OFS_BASE = "https://disseminate.stats.swiss/rest"
OFS_PAGE = "https://www.bfs.admin.ch/asset/fr/je-f-19.03.02.02.02.08.01"
OFS_EXPLORER = "https://stats.swiss/vis?lc=fr&df%5bds%5d=disseminate&df%5bid%5d=DF_SUS_NATIONALITIES_LAWS&df%5bag%5d=CH1.SUS&df%5bvs%5d=1.0.0"
OFS_STRUCTURE_ACCEPT = "application/vnd.sdmx.structure+json; version=1.0; charset=utf-8"
OFS_DATA_ACCEPT = "application/vnd.sdmx.data+csv; version=1.0.0"
OFS_LAWS = {"_S_311_0": "scc", "_S_812_121": "narc", "_S_741_01": "rta"}  # penal code, narcotics act, road traffic act (FNIA left out: foreigners only)
OFS_NATIONALITIES_URL = f"{OFS_BASE}/dataflow/CH1.SUS/DF_SUS_NATIONALITIES_LAWS/1.0.0?references=descendants&detail=full"
OFS_PERSONS_URL = f"{OFS_BASE}/data/CH1.SUS,DF_SUS_PERSONS,1.0.0/_S_311_0.....?startPeriod=2007"
_SEX = {"1": "M", "2": "F", "_T": "T", "3": "U"}
_POP = {"1": "res", "2": "other", "_T": "all"}


def ofs_names(structure: dict) -> dict[str, str]:
    """Nationality code (8xxx) -> English name from the table's codelist."""
    for cl in structure["data"]["codelists"]:
        if cl["id"] == "CL_HST_TOTAL_QSTATE":
            return {c["id"]: (c.get("names") or {}).get("en") or c.get("name", "") for c in cl["codes"]}
    raise ValueError("OFS: nationality codelist missing")


def ofs_sus_rows(csv_text: str, names: dict[str, str], law: str, *, population: bool) -> tuple[list[dict], dict[str, str]]:
    """Persons convicted (and, once, the resident population) by nationality, population category, sex and age.

    Returns the rows and the nationalities that could not be matched to a country (code -> name), kept under `?code`."""
    out: list[dict] = []
    unmapped: dict[str, str] = {}
    seen: set[tuple] = set()
    for r in csv.DictReader(io.StringIO(csv_text)):
        if r["UNIT_MEASURE"] != "PS" or r["OBS_VALUE"] in ("", None):
            continue
        measure = "convicted" if r["OBS_SOURCE"] == "SUS" else "population" if r["OBS_SOURCE"] == "STATPOP" else None
        if measure is None or (measure == "population" and not population):
            continue
        code = r["DECISION_CONVICTED_NATIONALITY"]
        if code == "_T":
            nat = TOTAL
        else:
            nat = "CHE" if code == "8100" else iso3(names.get(code))
            if nat is None:
                nat = f"?{code}"
                unmapped[code] = names.get(code, "")
        key = (law, nat, _POP.get(r["DECISION_CONVICTED_POPULATION"]), _SEX.get(r["DECISION_CONVICTED_SEX"]), r["DECISION_CONVICTED_AGE"], measure)
        if key in seen:  # two codes can land on one country (former states): the first stands, never summed twice
            continue
        seen.add(key)
        out.append({"law": law, "nat": nat, "pop": key[2], "sex": key[3], "age": key[4], "measure": measure, "time": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])})
    return out, unmapped


OFS_SUS_DIMS = ["law", "nat", "pop", "sex", "age", "measure", "time"]
OFS_GROUPS = {"_S_SWISS": "swiss", "_S_RESIDENT": "foreign_resident", "_S_NOT_RESIDENT": "foreign_nonresident", "_U_NOT_SWISS": "foreign_all", "_T": "all"}


def ofs_persons_rows(csv_text: str) -> list[dict]:
    """DF_SUS_PERSONS: persons convicted under the penal code by citizenship group and sex, all courts, one row per year."""
    best: dict[tuple, dict] = {}
    for r in csv.DictReader(io.StringIO(csv_text)):
        if r["CONVICTED_AGE_STATS"] != "COUNT" or r["UNIT_MEASURE"] != "PS" or r["OBS_VALUE"] in ("", None):
            continue
        group = OFS_GROUPS.get(r["DECISION_CONVICTED_CITIZENSHIP"])
        if group is None or r["COURT_MEMBERSHIP"] not in ("_T", "T", "CH"):
            continue
        key = (group, _SEX.get(r["DECISION_CONVICTED_SEX"]), r["TIME_PERIOD"])
        best[key] = {"group": key[0], "sex": key[1], "time": key[2], "value": float(r["OBS_VALUE"])}
    return list(best.values())


OFS_PERSONS_DIMS = ["group", "sex", "time"]


# ---------------------------------------------------------------- Germany, BKA PKS table T62

BKA_FILE = ("https://www.bka.de/SharedDocs/Downloads/DE/Publikationen/PolizeilicheKriminalstatistik/{year}/Bund/Tatverdaechtige/"
            "BU-TV-22-T62-TV-Staatsangehoerigkeiten_xls.xlsx?__blob=publicationFile")
BKA_PAGE = "https://www.bka.de/DE/AktuelleInformationen/StatistikenLagebilder/PolizeilicheKriminalstatistik/PKS{year}/PKSTabellen/BundTVNationalitaet/bundTVNationalitaet.html"
# The offences kept: all, all without the residence-law offences foreigners alone can commit (890000), violent crime, homicide,
# sexual offences, bodily harm and threats (Rohheitsdelikte), fraud and forgery, narcotics.
BKA_OFFENCES = {"------": "all", "890000": "all_excl_residence_law", "892000": "violent", "892500": "homicide", "100000": "sexual",
                "200000": "assault_threat", "500000": "property_fraud", "730000": "narcotics", "725000": "residence_law"}
BKA_SPECIAL = {"tatverdachtige insgesamt": TOTAL, "nichtdeutsche insgesamt": "_ND", "ohne angabe": UNKNOWN, "staatenlos": STATELESS,
               "ungeklart": UNKNOWN, "unbekannt": UNKNOWN, "deutschland": "DEU"}
BKA_ALIASES = {"serbien und montenegro": "YUG", "jugoslawien": "YUG", "kosovo": "KOS", "palastinensische gebiete": "PSE",
               "syrien arabische republik": "SYR", "kongo dem rep": "COD", "kongo republik": "COG", "kongo": "COG",
               "demokratische republik kongo": "COD", "usa": "USA", "korea sud": "KOR", "korea nord": "PRK", "weissrussland": "BLR", "belarus": "BLR",
               "turkei": "TUR", "bosnien und herzegowina": "BIH", "nordmazedonien": "MKD", "mazedonien": "MKD", "vereinigtes konigreich": "GBR",
               "grossbritannien": "GBR", "gambia": "GMB", "kap verde": "CPV", "tschechische republik": "CZE", "tschechien": "CZE", "moldau": "MDA",
               "moldawien": "MDA", "grossbritannien nordirland": "GBR", "korea volksrepublik": "PRK", "mikronesien": "FSM",
               "st vincent": "VCT", "sudan ohne sudsudan": "SDN", "swasiland": "SWZ", "weissrussland belarus": "BLR", "zypern": "CYP",
               "russische foderation": "RUS", "vietnam": "VNM", "burma myanmar": "MMR", "myanmar": "MMR"}


def bka_nat(name: str) -> str | None:
    key = norm(name)
    return BKA_SPECIAL.get(key) or BKA_ALIASES.get(key) or iso3(name)


def bka_t62_rows(data: bytes, year: int) -> tuple[list[dict], list[str]]:
    """Suspects by offence and nationality from one year's T62 workbook. Returns rows and the unmatched nationality names."""
    sheet = next(iter(read_xlsx(data).values()))
    rows = [cells for _n, cells in sheet]
    header_at = next(i for i, c in enumerate(rows) if c and c[0] == "Schlüssel")
    names = rows[header_at]
    nat: dict[int, str] = {}
    unmapped: list[str] = []
    for j, name in enumerate(names):
        if j < 2 or not name:
            continue
        code = bka_nat(str(name))
        if code is None:
            unmapped.append(str(name))
            code = f"?{norm(str(name))}"
        nat[j] = code
    out: list[dict] = []
    for cells in rows[header_at + 1:]:
        key = str(cells[0]).strip() if cells and cells[0] is not None else ""
        if key not in BKA_OFFENCES:
            continue
        for j, code in nat.items():
            v = cells[j] if j < len(cells) else None
            if v in (None, ""):
                continue
            out.append({"offence": BKA_OFFENCES[key], "nat": code, "time": str(year), "value": float(v)})
    return out, unmapped


BKA_DIMS = ["offence", "nat", "time"]


# ---------------------------------------------------------------- Netherlands, CBS

CBS_ODATA = "https://opendata.cbs.nl/ODataApi/OData"
CBS_SUSPECTS = "85656NED"
CBS_POPULATION = "85510NED"
CBS_PAGE = "https://opendata.cbs.nl/statline/#/CBS/nl/dataset/{table}/table"
CBS_OFFENCES = {"T001161": "all", "CRI3000": "violent_sexual", "CRI3100": "assault", "CRI3300": "sexual", "CRI1000": "property", "CRI6000": "drugs"}
CBS_SEX = {"T001038": "T", "3000": "M", "4000": "F"}
CBS_RES = {"T001479": "all", "A047155": "resident", "A047154": "nonresident"}
# CBS names nationalities by adjective (Dutch). Only the nationalities of the suspects table (at least 1 000 residents on 1 January 2022).
NL_NATIONALITIES = {
    "nederlands": "NLD", "afghaans": "AFG", "albanees": "ALB", "algerijns": "DZA", "amerikaans": "USA", "argentijns": "ARG", "armeens": "ARM",
    "australisch": "AUS", "azerbeidzjaans": "AZE", "wit russisch": "BLR", "belgisch": "BEL", "bengalees": "BGD", "braziliaans": "BRA", "brits": "GBR",
    "bulgaars": "BGR", "duits": "DEU", "bosnisch": "BIH", "congolees d r": "COD", "burger van republiek noord macedonie": "MKD", "canadees": "CAN",
    "chileens": "CHL", "chinees": "CHN", "colombiaans": "COL", "cyprisch": "CYP", "deens": "DNK", "dominicaans": "DOM", "egyptisch": "EGY",
    "eritrees": "ERI", "estisch": "EST", "ethiopisch": "ETH", "filippijns": "PHL", "fins": "FIN", "frans": "FRA", "georgisch": "GEO", "ghanees": "GHA",
    "grieks": "GRC", "hongaars": "HUN", "iers": "IRL", "indiaas": "IND", "indonesisch": "IDN", "iraaks": "IRQ", "iraans": "IRN", "israelisch": "ISR",
    "italiaans": "ITA", "japans": "JPN", "jemenitisch": "YEM", "kaapverdisch": "CPV", "kazachs": "KAZ", "keniaans": "KEN", "kroatisch": "HRV",
    "lets": "LVA", "libanees": "LBN", "litouws": "LTU", "luxemburgs": "LUX", "maleisisch": "MYS", "marokkaans": "MAR", "mexicaans": "MEX",
    "moldavisch": "MDA", "nieuw zeelands": "NZL", "nigeriaans": "NGA", "noors": "NOR", "oekraiens": "UKR", "oostenrijks": "AUT", "pakistaans": "PAK",
    "peruaans": "PER", "pools": "POL", "portugees": "PRT", "roemeens": "ROU", "russisch": "RUS", "servisch": "SRB", "sierra leoons": "SLE",
    "singaporees": "SGP", "sloveens": "SVN", "slowaaks": "SVK", "soedanees": "SDN", "somalisch": "SOM", "spaans": "ESP", "sri lankaans": "LKA",
    "surinaams": "SUR", "syrisch": "SYR", "taiwanees": "TWN", "thais": "THA", "tsjechisch": "CZE", "tunesisch": "TUN", "turks": "TUR",
    "ugandees": "UGA", "venezolaans": "VEN", "vietnamees": "VNM", "zuid afrikaans": "ZAF", "zuid koreaans": "KOR", "zweeds": "SWE", "zwitsers": "CHE",
    "kosovaars": "KOS", "montenegrijns": "MNE", "irakees": "IRQ", "burger van kosovo": "KOS", "libisch": "LBY", "angolees": "AGO",
    "ivoriaans": "CIV", "kameroens": "CMR", "senegalees": "SEN", "guineese": "GIN", "gambiaans": "GMB", "togolees": "TGO", "burundisch": "BDI",
    "rwandees": "RWA", "tanzaniaans": "TZA", "zimbabwaans": "ZWE", "zambiaans": "ZMB", "mauritaans": "MRT", "kirgizisch": "KGZ", "oezbeeks": "UZB",
    "mongools": "MNG", "nepalees": "NPL", "jordaans": "JOR", "koeweits": "KWT", "saoedi arabisch": "SAU", "cubaans": "CUB", "ecuadoraans": "ECU",
    "haitiaans": "HTI", "jamaicaans": "JAM", "bolivaans": "BOL", "paraguayaans": "PRY", "uruguayaans": "URY", "maltees": "MLT", "ijslands": "ISL",
    "macedonisch": "MKD", "bosnisch herzegovijns": "BIH", "joegoslavisch": "YUG", "sovjet unie": "SUN",
}
NL_SPECIAL = {"totaal": TOTAL, "overige nationaliteiten": OTHERS, "staatloos onbekend": STATELESS}


def cbs_nat(title: str) -> str | None:
    key = norm(title)
    return NL_SPECIAL.get(key) or NL_NATIONALITIES.get(key) or iso3(title)


def cbs_period_year(period: str) -> str | None:
    m = re.match(r"^(\d{4})JJ00$", period.strip())
    return m.group(1) if m else None


def cbs_suspect_rows(items: list[dict], nat_titles: dict[str, str]) -> tuple[list[dict], list[str]]:
    """Registrations of suspects: offence, sex, residence, nationality, year. `items` are UntypedDataSet records."""
    out: list[dict] = []
    unmapped: dict[str, str] = {}
    for it in items:
        offence = CBS_OFFENCES.get(it["TypeMisdrijf"].strip())
        sex = CBS_SEX.get(it["Geslacht"].strip())
        res = CBS_RES.get(it["WelGeenWoonadresInNederland"].strip())
        year = cbs_period_year(it["Perioden"])
        raw = it.get("RegistratiesVanVerdachten_1")
        if None in (offence, sex, res, year) or raw in (None, ""):
            continue
        key = it["Nationaliteit"].strip()
        code = cbs_nat(nat_titles.get(key, ""))
        if code is None:
            unmapped[key] = nat_titles.get(key, "")
            continue
        out.append({"offence": offence, "sex": sex, "res": res, "nat": code, "time": year, "value": float(str(raw).strip())})
    return out, sorted(unmapped.values())


def cbs_population_rows(items: list[dict], nat_titles: dict[str, str]) -> list[dict]:
    out: list[dict] = []
    for it in items:
        sex = CBS_SEX.get(it["Geslacht"].strip())
        m = re.match(r"^(\d{4})JJ00$", it["Perioden"].strip())
        raw = it.get("BevolkingOp1Januari_1")
        if sex is None or not m or raw in (None, ""):
            continue
        code = cbs_nat(nat_titles.get(it["Nationaliteit"].strip(), ""))
        if code is None:
            continue
        out.append({"sex": sex, "nat": code, "time": m.group(1), "value": float(str(raw).strip())})
    return out


CBS_SUSPECT_DIMS = ["offence", "sex", "res", "nat", "time"]
CBS_POP_DIMS = ["sex", "nat", "time"]


# ---------------------------------------------------------------- Denmark, Statistics Denmark

DST_API = "https://api.statbank.dk/v1"
DST_GUILTY = "STRAFNA3"
DST_POP = "FOLK2"
DST_PAGE = "https://www.statbank.dk/{table}"
DST_AGE_GROUPS = (("15-29", 15, 29), ("30-49", 30, 49), ("50-79", 50, 79), ("15-79", 15, 79))
DST_ALIASES = {"denmark": "DNK", "usa": "USA", "other countries total": None, "total": TOTAL, "other countries western": "_OTHER_WESTERN",
               "other countries non western": "_OTHER_NONWESTERN", "yugoslavia federal republic": "YUG"}


def dst_nat(name: str) -> str | None:
    key = norm(name)
    if key in DST_ALIASES:
        return DST_ALIASES[key]
    return iso3(name)


def dst_csv(text: str) -> list[dict]:
    text = text.lstrip("﻿")
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def dst_guilty_rows(text: str) -> tuple[list[dict], list[str]]:
    out: list[dict] = []
    unmapped: set[str] = set()
    sex = {"men": "M", "women": "F"}
    age = {"age total": "15-79", "15 29 years": "15-29", "30 49 years": "30-49", "50 79 years": "50-79"}
    for r in dst_csv(text):
        s, a = sex.get(r["KOEN"].strip().lower()), age.get(norm(r["ALDER"]))
        if not s or not a or r["INDHOLD"] in ("", "..", "-"):
            continue
        nat = dst_nat(r["IELAND"])
        if nat is None:
            unmapped.add(r["IELAND"])
            continue
        out.append({"sex": s, "age": a, "nat": nat, "time": r["TID"].strip(), "value": float(r["INDHOLD"])})
    return out, sorted(unmapped)


def dst_population_rows(text: str) -> list[dict]:
    """FOLK2 single years of age summed into the guilty-persons age bands, by sex, country of origin and year."""
    sums: dict[tuple, float] = {}
    sex = {"men": "M", "women": "F"}
    for r in dst_csv(text):
        m = re.match(r"^(\d+) years?$", r["ALDER"].strip())
        s = sex.get(r["KØN"].strip().lower())
        if not m or not s or r["INDHOLD"] in ("", "..", "-"):
            continue
        nat = dst_nat(r["IELAND"])
        if nat is None:
            continue
        year = int(m.group(1))
        for label, lo, hi in DST_AGE_GROUPS:
            if lo <= year <= hi:
                k = (s, label, nat, r["TID"].strip())
                sums[k] = sums.get(k, 0.0) + float(r["INDHOLD"])
    return [{"sex": k[0], "age": k[1], "nat": k[2], "time": k[3], "value": v} for k, v in sums.items()]


DST_DIMS = ["sex", "age", "nat", "time"]


# ---------------------------------------------------------------- Italy, ISTAT (SDMX)

ISTAT_BASE = "https://esploradati.istat.it/SDMXWS/rest"
ISTAT_ALLEGED = "73_230_DF_DCCV_AUTVITTPS_7"
ISTAT_ALLEGED_PAGE = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,Z0810JUS,1.0/JUS_JUSOFFEN"
ISTAT_ACCEPT = "application/vnd.sdmx.data+csv;version=1.0.0"
ISTAT_CITIZEN = {"ITL": "national", "FRG": "foreign", "TOTAL": "all"}


def istat_alleged_rows(csv_text: str) -> list[dict]:
    """Alleged offenders reported by the police to the judicial authority: Italy, offence, sex, age, Italian or foreign citizenship."""
    out: list[dict] = []
    for r in csv.DictReader(io.StringIO(csv_text)):
        cit = ISTAT_CITIZEN.get(r["CITIZENSHIP"])
        if cit is None or r["OBS_VALUE"] in ("", None) or r["COUNTRY_CITIZEN"] != "WORLD":
            continue
        sex = {"1": "M", "2": "F", "9": "T"}.get(r["SEX"])
        if sex is None:
            continue
        out.append({"offence": r["TYPE_CRIME"], "sex": sex, "age": r["AGE"], "cit": cit, "time": r["TIME_PERIOD"], "value": float(r["OBS_VALUE"])})
    return out


ISTAT_DIMS = ["offence", "sex", "age", "cit", "time"]


# ---------------------------------------------------------------- Spain, INE (Tempus)

INE_TEMPUS = "https://servicios.ine.es/wstempus/js/ES"
INE_CONVICTED = "25645"       # adults convicted by sex, age and nationality (CONA)
INE_VGD_DOMESTIC = "28402"    # domestic violence, convicted, by sex and place of birth (VGD)
INE_VGD_GENDER = "28305"      # gender violence, convicted, by place of birth (VGD)
INE_PAGE = "https://www.ine.es/jaxiT3/Tabla.htm?t={table}"
INE_REGION = {"espana": "ES", "española": "ES", "total": TOTAL, "europa": "EUR", "de africa": "AFR", "de america": "AME", "de asia": "ASI",
              "de oceania": "OCE", "africa": "AFR", "america": "AME", "asia": "ASI", "oceania": "OCE",
              "pais de la ue27 2020 sin espana": "EU27", "ue27 2020 sin espana": "EU27",
              "pais de europa menos ue27 2020": "EUR_NONEU", "europa menos ue27 2020": "EUR_NONEU", "espanola": "ES"}


def ine_origin(label: str) -> str | None:
    return INE_REGION.get(norm(label))


def ine_convicted_rows(data: list[dict]) -> list[dict]:
    """Table 25645: name = 'sex. nationality. age. Dato base. Total Nacional.' per series, one value per year."""
    out: list[dict] = []
    for series in data:
        parts = [p.strip() for p in series["Nombre"].split(". ")]
        if len(parts) < 5 or "Dato base" not in parts:
            continue
        sex = {"hombres": "M", "mujeres": "F", "total": "T"}.get(parts[0].lower())
        nat = ine_origin(parts[1])
        age = parts[2]
        if sex is None or nat is None:
            continue
        for v in series["Data"]:
            if v.get("Valor") is not None and not v.get("Secreto"):
                out.append({"sex": sex, "nat": nat, "age": age, "time": str(v["Anyo"]), "value": float(v["Valor"])})
    return out


def ine_violence_rows(data: list[dict], kind: str) -> list[dict]:
    """Tables 28402 / 28305: 'Total Nacional. Violencia ... Condenados. <place of birth>[. <sex>].' per series."""
    out: list[dict] = []
    for series in data:
        parts = [p.strip() for p in series["Nombre"].split(". ")]
        if not parts or parts[0].lower() != "total nacional" or "Dato base" not in parts:
            continue
        tail = parts[parts.index("Condenados") + 1:] if "Condenados" in parts else []
        tail = [t.rstrip(". ") for t in tail if t.strip(". ")]
        if not tail:
            continue
        origin = ine_origin(tail[0])
        sex = "T"
        if len(tail) > 1:
            sex = {"hombres": "M", "mujeres": "F", "total": "T"}.get(tail[1].lower(), "T")
        if origin is None:
            continue
        for v in series["Data"]:
            if v.get("Valor") is not None and not v.get("Secreto"):
                out.append({"kind": kind, "origin": origin, "sex": sex, "time": str(v["Anyo"]), "value": float(v["Valor"])})
    return out


INE_CONVICTED_DIMS = ["sex", "nat", "age", "time"]
INE_VIOLENCE_DIMS = ["kind", "origin", "sex", "time"]


# ---------------------------------------------------------------- France, SSMSI (data.gouv.fr)

SSMSI_MEC_DATASET = "https://www.data.gouv.fr/api/1/datasets/694410b463279019f7d95486/"
SSMSI_MEC_PAGE = "https://www.data.gouv.fr/datasets/694410b463279019f7d95486"
SSMSI_NATIONALITY = {"Française": "national", "Etrangère": "foreign", "Ensemble": "all"}
SSMSI_SEX = {"H": "M", "F": "F", "Ensemble": "T"}


def ssmsi_mec_resource(dataset: dict) -> dict:
    for r in dataset.get("resources", []):
        if "mec" in (r.get("url") or "").lower() and (r.get("format") or "").lower() == "xlsx":
            return {"url": r["url"], "last_modified": r.get("last_modified"), "title": r.get("title")}
    raise ValueError("SSMSI: the mis en cause workbook is not listed any more")


def ssmsi_mec_rows(data: bytes) -> list[dict]:
    """People charged for offences cleared up: indicator, year, sex, age band, French or foreign. Statistical secrecy: left out."""
    sheet = read_xlsx(data)["MEC"]
    out: list[dict] = []
    seen: set[tuple] = set()  # an age band is listed once for adults and once for all: the same people, kept once
    for n, cells in sheet:
        if n == 1 or not cells or len(cells) < 7:
            continue
        indicator, year, sex, age, majorite, nat, count = cells[:7]
        age = str(age).strip()
        if age == "Ensemble":  # the same label appears for all ages, for adults and for minors: keep them apart
            age = {"Majeur": "Majeurs", "Mineur": "Mineurs"}.get(str(majorite), "Ensemble")
        if not isinstance(count, float) or nat not in SSMSI_NATIONALITY or sex not in SSMSI_SEX:
            continue
        key = (str(indicator), SSMSI_SEX[sex], age, SSMSI_NATIONALITY[nat], str(int(year)))
        if key in seen:
            continue
        seen.add(key)
        out.append({"indicator": key[0], "sex": key[1], "age": age, "nat": key[3], "time": key[4], "value": count})
    return out


SSMSI_DIMS = ["indicator", "sex", "age", "nat", "time"]


# ---------------------------------------------------------------- Eurostat: population by citizenship

EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/migr_pop1ctz"
EUROSTAT_VIEW = "https://ec.europa.eu/eurostat/databrowser/view/migr_pop1ctz/default/table?lang=fr"
POP_GEOS = ("CH", "DE", "DK", "ES", "FR", "IT", "NL")
POP_GROUPS = ("TOTAL", "NAT", "FOR_STLS", "EU27_2020_FOR", "NEU27_2020_FOR")


EUROSTAT_BIRTH = EUROSTAT.replace("migr_pop1ctz", "migr_pop3ctb")


def eurostat_pop_urls() -> dict[str, str]:
    """Three requests: every citizenship, all ages and both sexes together; the groups only, by sex and age; the country of birth."""
    geo = "&".join(f"geo={g}" for g in POP_GEOS)
    return {
        "birth": f"{EUROSTAT_BIRTH}?format=JSON&lang=en&{geo}&sex=T&age=TOTAL&c_birth=TOTAL&c_birth=NAT&c_birth=FOR&c_birth=EU27_2020_FOR&c_birth=NEU27_2020_FOR&sinceTimePeriod=2007",
        "countries": f"{EUROSTAT}?format=JSON&lang=en&{geo}&sex=T&age=TOTAL&sinceTimePeriod=2007",
        "groups": f"{EUROSTAT}?format=JSON&lang=en&{geo}&" + "&".join(f"citizen={c}" for c in POP_GROUPS) + "&sinceTimePeriod=2013",
    }


def eurostat_pop_rows(payload: dict) -> list[dict]:
    rows, _labels = jsonstat_rows(payload)
    return [{"citizen": r.get("citizen") or r["c_birth"], "age": r["age"], "sex": r["sex"], "geo": r["geo"], "time": str(r["time"]), "value": r["value"]} for r in rows]


EUROSTAT_POP_DIMS = ["citizen", "age", "sex", "geo", "time"]
