"""Offending by nationality: the parsers on small tables built like the publishers', and the collector's behaviour when a source fails."""

import io

import openpyxl

from homelab_datalake.assets import crime_nationality as asset
from homelab_datalake.parsing import crime_nationality as c


def test_country_names_in_every_language_reach_one_code():
    assert c.iso3("Türkiye") == c.iso3("Turkey") == c.iso3("Türkei") == c.iso3("Turquie") == "TUR"
    assert c.iso3("Bosnien und Herzegowina") == c.iso3("Bosnia and Herzegovina") == "BIH"
    assert c.iso3("Korea, South") == "KOR"
    assert c.iso3("Côte d'Ivoire") == c.iso3("Elfenbeinküste") == "CIV"
    assert c.iso3("Atlantis") is None
    assert c.iso3_from_iso2("EL") == "GRC" and c.iso3_from_iso2("UK") == "GBR" and c.iso3_from_iso2("RO") == "ROU"


SUS = """DATAFLOW,OFFENCE_LAW_ARTICLE,DECISION_CONVICTED_NATIONALITY,DECISION_CONVICTED_POPULATION,DECISION_CONVICTED_SEX,DECISION_CONVICTED_AGE,OBS_SOURCE,UNIT_MEASURE,FREQ,TIME_PERIOD,OBS_VALUE
x,_S_311_0,8100,1,1,Y18T29,SUS,PS,A,2024,2500
x,_S_311_0,8100,1,1,Y18T29,STATPOP,PS,A,2024,300000
x,_S_311_0,8100,1,1,Y18T29,SUS,PT,A,2024,0.8
x,_S_311_0,8212,1,1,Y18T29,SUS,PS,A,2024,900
x,_S_311_0,8212,1,1,Y18T29,STATPOP,PS,A,2024,40000
x,_S_311_0,8999,1,1,Y18T29,SUS,PS,A,2024,5
x,_S_311_0,_T,_T,_T,_T,SUS,PS,A,2024,100000
x,_S_311_0,8212,2,1,Y18T29,SUS,PS,A,2024,
"""
NAMES = {"8100": "Switzerland", "8212": "France", "8999": "Unknown place"}


def test_ofs_rows_keep_counts_and_population_not_percentages():
    rows, unmapped = c.ofs_sus_rows(SUS, NAMES, "scc", population=True)
    got = {(r["nat"], r["measure"]): r["value"] for r in rows}
    assert got[("CHE", "convicted")] == 2500 and got[("CHE", "population")] == 300000
    assert got[("FRA", "convicted")] == 900 and got[(c.TOTAL, "convicted")] == 100000
    assert len(rows) == 6 and unmapped == {"8999": "Unknown place"}      # the % row and the empty row are gone
    assert [r for r in rows if r["nat"] == "?8999"]
    assert all(r["measure"] != "population" for r in c.ofs_sus_rows(SUS, NAMES, "scc", population=False)[0])


def test_ofs_persons_groups_citizenship_and_sex():
    text = ("OFFENCE_LAW_ARTICLE,DECISION_CONVICTED_SEX,DECISION_CONVICTED_CITIZENSHIP,COURT_MEMBERSHIP,CONVICTED_AGE_STATS,UNIT_MEASURE,TIME_PERIOD,OBS_VALUE\n"
            "_S_311_0,1,_S_SWISS,_T,COUNT,PS,2024,40000\n_S_311_0,1,_S_RESIDENT,_T,COUNT,PS,2024,18000\n"
            "_S_311_0,1,_S_RESIDENT,_T,MEAN,YR,2024,33\n_S_311_0,1,_S_RESIDENT,12,COUNT,PS,2024,10\n")
    rows = c.ofs_persons_rows(text)
    assert {(r["group"], r["sex"], r["value"]) for r in rows} == {("swiss", "M", 40000.0), ("foreign_resident", "M", 18000.0)}


def workbook(rows) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    for r in rows:
        ws.append(r)
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def test_bka_t62_reads_offences_by_nationality_and_flags_what_it_cannot_match():
    data = workbook([
        ["Polizeiliche Kriminalstatistik"], ["x"], ["y"], [None, None, None, "...", "000", "121"],
        ["Schlüssel", "Straftat", "Tatverdächtige insgesamt", "Nichtdeutsche insgesamt", "Deutschland", "Türkei", "Atlantis"],
        ["------", "Straftaten insgesamt", 100, 40, 60, 12, 3],
        ["890000", "ohne Aufenthaltsrecht", 90, 30, 60, 10, "0"],
        ["010000", "Mord", 1, 1, 0, 0, 0],
    ])
    rows, unmapped = c.bka_t62_rows(data, 2025)
    got = {(r["offence"], r["nat"]): r["value"] for r in rows}
    assert got[("all", c.TOTAL)] == 100 and got[("all", "_ND")] == 40 and got[("all", "DEU")] == 60 and got[("all", "TUR")] == 12
    assert got[("all_excl_residence_law", "TUR")] == 10 and ("homicide", "TUR") not in got   # 010000 is not a kept offence
    assert unmapped == ["Atlantis"]


def test_cbs_registrations_by_nationality_and_residence():
    titles = {"NAT9387": "Marokkaans", "T001059": "Totaal", "NAT9999": "Overige nationaliteiten"}
    items = [
        {"TypeMisdrijf": "CRI3000", "Geslacht": "T001038 ", "WelGeenWoonadresInNederland": "A047155", "Nationaliteit": "NAT9387", "Perioden": "2024JJ00", "RegistratiesVanVerdachten_1": "     410"},
        {"TypeMisdrijf": "CRI3000", "Geslacht": "3000    ", "WelGeenWoonadresInNederland": "T001479", "Nationaliteit": "T001059", "Perioden": "2024JJ00", "RegistratiesVanVerdachten_1": "  9000"},
        {"TypeMisdrijf": "CRI3000", "Geslacht": "3000", "WelGeenWoonadresInNederland": "T001479", "Nationaliteit": "NAT0000", "Perioden": "2024JJ00", "RegistratiesVanVerdachten_1": "5"},
        {"TypeMisdrijf": "CRI3000", "Geslacht": "3000", "WelGeenWoonadresInNederland": "T001479", "Nationaliteit": "NAT9387", "Perioden": "2024KW01", "RegistratiesVanVerdachten_1": "5"},
    ]
    rows, unmapped = c.cbs_suspect_rows(items, titles)
    assert [(r["offence"], r["sex"], r["res"], r["nat"], r["value"]) for r in rows] == [("violent_sexual", "T", "resident", "MAR", 410.0), ("violent_sexual", "M", "all", c.TOTAL, 9000.0)]
    pop = c.cbs_population_rows([{"Geslacht": "T001038", "Leeftijd": "10000", "Nationaliteit": "NAT9387", "Perioden": "2024JJ00", "BevolkingOp1Januari_1": " 400000"}], titles)
    assert pop == [{"sex": "T", "nat": "MAR", "time": "2024", "value": 400000.0}]


def test_denmark_guilty_and_population_age_bands():
    guilty = "﻿KOEN;ALDER;IELAND;TID;INDHOLD\nMen;15-29 years;Syria;2024;300\nMen;Age, total;Denmark;2024;9000\nWomen;Age, total;Total;2024;40\nMen;Age, total;Czechoslovakia;2024;2\n"
    rows, unmapped = c.dst_guilty_rows(guilty)
    assert {(r["sex"], r["age"], r["nat"]) for r in rows} == {("M", "15-29", "SYR"), ("M", "15-79", "DNK"), ("F", "15-79", c.TOTAL), ("M", "15-79", "CSK")}
    pop = c.dst_population_rows("ALDER;KØN;IELAND;TID;INDHOLD\n20 years;Men;Syria;2024;1000\n40 years;Men;Syria;2024;500\n80 years;Men;Syria;2024;9\n")
    got = {(r["age"], r["nat"]): r["value"] for r in pop}
    assert got[("15-29", "SYR")] == 1000 and got[("30-49", "SYR")] == 500 and got[("15-79", "SYR")] == 1500   # 80 years is outside 15-79


def test_istat_alleged_offenders_italian_or_foreign():
    text = ("DATAFLOW,FREQ,REF_AREA,DATA_TYPE,TYPE_CRIME,SEX,AGE,CITIZENSHIP,COUNTRY_CITIZEN,TIME_PERIOD,OBS_VALUE\n"
            "x,A,IT,OFFEND,CP572,9,TOTAL,FRG,WORLD,2024,3000\nx,A,IT,OFFEND,CP572,9,TOTAL,ITL,WORLD,2024,9000\nx,A,IT,OFFEND,CP572,9,TOTAL,EU27,WORLD,2024,1\n")
    assert {(r["cit"], r["value"]) for r in c.istat_alleged_rows(text)} == {("foreign", 3000.0), ("national", 9000.0)}


def test_ine_names_become_dimensions():
    convicted = [{"Nombre": "Hombres. De Africa. De 26 a 30 años. Dato base. Total Nacional. ", "Data": [{"Anyo": 2025, "Valor": 120.0, "Secreto": False}]},
                 {"Nombre": "Total. Total. Total. Variación interanual. Total Nacional. ", "Data": [{"Anyo": 2025, "Valor": 1.0}]}]
    assert c.ine_convicted_rows(convicted) == [{"sex": "M", "nat": "AFR", "age": "De 26 a 30 años", "time": "2025", "value": 120.0}]
    violence = [{"Nombre": "Total Nacional. Violencia de genero. Sentencias firmes. Dato base. Condenados. España.", "Data": [{"Anyo": 2025, "Valor": 24704.0}]},
                {"Nombre": "Total Nacional. Violencia de genero. Sentencias firmes. Dato base. Condenados. África.", "Data": [{"Anyo": 2025, "Valor": 3740.0}]}]
    got = {(r["origin"], r["value"]) for r in c.ine_violence_rows(violence, "gender")}
    assert got == {("ES", 24704.0), ("AFR", 3740.0)}


def test_ssmsi_mis_en_cause_leaves_out_statistical_secrecy():
    data = workbook([["indicateur", "annee", "sexe", "age", "majorite", "nationalite", "nombre", "diffusion"],
                     ["Homicides", 2024, "H", " 18 à 29 ans", "Majeur", "Française", 50, "Diffusion"],
                     ["Homicides", 2024, "H", " 18 à 29 ans", "Majeur", "Etrangère", 20, "Diffusion"],
                     ["Homicides", 2024, "H", " 18 à 29 ans", "Majeur", "Etrangère", "secr.", "Secret secondaire"],
                     ["Homicides", 2024, "H", "Ensemble", "Majeur", "Etrangère", 18, "Diffusion"],
                     ["Homicides", 2024, "H", "Ensemble", "Mineur", "Etrangère", 2, "Diffusion"],
                     ["Homicides", 2024, "H", "Ensemble", "Ensemble", "Etrangère", 20, "Diffusion"],
                     ["Homicides", 2024, "H", " 18 à 29 ans", "Ensemble", "Française", 50, "Diffusion"]])
    wb = openpyxl.load_workbook(io.BytesIO(data))
    wb.active.title = "MEC"
    out = io.BytesIO()
    wb.save(out)
    rows = c.ssmsi_mec_rows(out.getvalue())
    assert [(r["nat"], r["value"], r["age"]) for r in rows] == [("national", 50.0, "18 à 29 ans"), ("foreign", 20.0, "18 à 29 ans"),
                                                                ("foreign", 18.0, "Majeurs"), ("foreign", 2.0, "Mineurs"), ("foreign", 20.0, "Ensemble")]


def test_eurostat_population_urls_and_rows():
    urls = c.eurostat_pop_urls()
    assert "geo=CH" in urls["countries"] and "age=TOTAL" in urls["countries"] and "citizen=NAT" in urls["groups"] and "citizen=FOR_STLS" in urls["groups"]
    payload = {"id": ["citizen", "age", "sex", "geo", "time"], "size": [1, 1, 1, 1, 1], "value": {"0": 100},
               "dimension": {"citizen": {"category": {"index": {"NAT": 0}}}, "age": {"category": {"index": {"TOTAL": 0}}}, "sex": {"category": {"index": {"T": 0}}},
                             "geo": {"category": {"index": {"FR": 0}}}, "time": {"category": {"index": {"2024": 0}}}}}
    assert c.eurostat_pop_rows(payload) == [{"citizen": "NAT", "age": "TOTAL", "sex": "T", "geo": "FR", "time": "2024", "value": 100.0}]


def test_block_and_ledger_rows_have_the_export_shape():
    parsed = [{"nat": "CHE", "time": "2024", "value": 5.0}]
    b = c.block("x", "label", "https://example.org", ["nat", "time"], [["CHE", "2024", 5.0]], "2026-11-15T00:00:00+00:00")
    assert b["source"] == "crime" and b["dims"] == ["nat", "time"] and b["rows"] == [["CHE", "2024", 5.0]] and b["status"] == "ok"
    row = c.ledger("x", parsed, ["nat", "time"], "https://example.org", "label", None)[0]
    assert row["dataset"] == "x" and row["period"] == "2024" and row["series_key"] == "nat=CHE" and row["source"] == "crime"


class Log:
    def __init__(self):
        self.warnings = []

    def warning(self, message, *args):
        self.warnings.append(message % args)

    info = warning


def test_every_source_fails_alone_and_is_reported(monkeypatch):
    """With the network down every source fails, none raises, and each is named so the export keeps yesterday's block (stale)."""
    def down(url, **_kwargs):
        raise RuntimeError("offline")

    monkeypatch.setattr(asset, "request", down)
    log = Log()
    rows, blocks, failed = asset.collect_crime("2026-11-15T00:00:00+00:00", log)
    assert rows == [] and blocks == {}
    assert set(failed) == {"crime:ch_sus", "crime:ch_persons", "crime:de_pks_t62", "crime:nl_suspects", "crime:nl_population", "crime:dk_guilty", "crime:dk_population", "crime:it_alleged",
                           "crime:es_convicted", "crime:fr_mec", "crime:pop", "crime:fr_bilan", "crime:fr_conjugal"}
    assert len([w for w in log.warnings if w.startswith('crime ')]) == len(failed)


def test_one_source_up_lands_its_blocks_and_ledger_rows(monkeypatch):
    payload = ('{"id":["citizen","age","sex","geo","time"],"size":[1,1,1,1,1],"value":{"0":100},"updated":"2026-11-13",'
               '"dimension":{"citizen":{"category":{"index":{"NAT":0}}},"age":{"category":{"index":{"TOTAL":0}}},"sex":{"category":{"index":{"T":0}}},'
               '"geo":{"category":{"index":{"FR":0}}},"time":{"category":{"index":{"2024":0}}}}}')

    def only_eurostat(url, **_kwargs):
        if "migr_pop" in url:
            return payload.encode()
        raise RuntimeError("offline")

    monkeypatch.setattr(asset, "request", only_eurostat)
    rows, blocks, failed = asset.collect_crime("2026-11-15T00:00:00+00:00", Log())
    assert set(blocks) == {"crime:pop_countries", "crime:pop_groups", "crime:pop_birth"} and "crime:pop" not in failed and len(failed) == 12
    assert len(rows) == 3 and {r["dataset"] for r in rows} == {"pop_countries", "pop_groups", "pop_birth"}
