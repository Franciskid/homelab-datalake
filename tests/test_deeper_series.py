"""Phase 7b inputs: workbooks kept as grids, the price index as annual averages, the OECD tax wedge, the Banque de France
holders, the SSMSI regional base and the latest Justice workbook."""

import io

import openpyxl

from homelab_datalake.parsing import curated_pensions
from homelab_datalake.parsing import deeper_series as ds
from homelab_datalake.parsing import public_spending as sp


def _wb(sheets):
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_a_workbook_is_kept_as_numeric_cells_and_labels():
    content = _wb({"Figure 1": [["Titre"], ["Origine", "Effectifs"], ["Afrique", 3665.0], ["Europe", 2386.0]], "Autre": [["x", 1.0]]})
    cells, texts = ds.grid_rows(content, ("Figure 1",))
    assert {(c["row"], c["col"], c["value"]) for c in cells} == {("3", "1", 3665.0), ("4", "1", 2386.0)}
    assert texts["Figure 1|3|0"] == "Afrique" and not any(k.startswith("Autre") for k in texts)
    block = ds.grid_block(ds.WORKBOOKS[0], cells, texts, "now")
    assert block["rows"][0] == ["Figure 1", "3", "1", 3665.0] and block["labels"]["text"]["Figure 1|2|1"] == "Effectifs"


def test_a_workbook_without_numbers_is_refused():
    try:
        ds.grid_rows(_wb({"A": [["texte seulement"]]}))
    except ValueError as error:
        assert "layout changed" in str(error)
    else:
        raise AssertionError


def _xml(values):
    obs = "".join(f'<Obs TIME_PERIOD="{p}" OBS_VALUE="{v}"/>' for p, v in values)
    return f'<message><Series IDBANK="x">{obs}</Series></message>'


def test_the_price_index_is_averaged_over_complete_years_only():
    vals = [(f"{y}-{m:02d}", 100 + (y - 1990)) for y in range(1990, 2025) for m in range(1, 13)] + [("2025-01", 130.0), ("2025-02", 131.0)]
    out = ds.ipc_annual(_xml(vals))
    assert out[0]["time"] == "1990" and out[-1]["time"] == "2024" and out[-1]["value"] == 134.0
    try:
        ds.ipc_annual(_xml(vals[:24]))
    except ValueError:
        pass
    else:
        raise AssertionError("fewer than twenty years must stop the run")


def test_the_oecd_wedge_maps_countries_and_adds_the_eu_mean():
    head = "DATAFLOW,REF_AREA,MEASURE,UNIT_MEASURE,HOUSEHOLD_TYPE,INCOME_PRINCIPAL,INCOME_SPOUSE,FREQ,TIME_PERIOD,OBS_VALUE\n"
    lines = "".join(f"x,{iso},AV_TW,PT_COS_LB,S_C0,AW100,_Z,A,2023,{40 + i}\n" for i, iso in enumerate(ds.ISO3_TO_GEO)) + "x,USA,AV_TW,PT_COS_LB,S_C0,AW100,_Z,A,2023,30\n"
    rows = ds.oecd_wedge_rows(head + lines)
    assert {r["geo"] for r in rows} >= {"FR", "EL", "EU27_2020"} and "US" not in {r["geo"] for r in rows}
    eu = next(r for r in rows if r["geo"] == "EU27_2020")
    assert eu["income"] == "AW100" and abs(eu["value"] - 50.5) < 1e-9  # mean of 40 .. 61


def test_banque_de_france_holders_and_ssmsi_regions_and_justice_workbook():
    rows = ds.bdf_rows({"series": {"docs": [{"period": ["1999-Q4", "2000-Q1"], "value": [28.0, "NA"]}]}})
    assert rows == [{"series": "non_residents_negotiable_debt_pct", "time": "1999-Q4", "value": 28.0}]
    csv_text = ("﻿\"Code_region\";\"annee\";\"indicateur\";\"unite_de_compte\";\"nombre\";\"taux_pour_mille\";\"insee_pop\"\n"
                "\"11\";\"2025\";\"Homicides\";\"Victime\";\"100\";\"0,0081\";\"12400000\"\n\"11\";\"2025\";\"Cambriolages de logement\";\"Fait\";\"NA\";\"NA\";\"12400000\"\n")
    out = ds.ssmsi_regional_rows(csv_text)
    assert {(r["mesure"], r["value"]) for r in out if r["indicateur"] == "Homicides"} == {("nombre", 100.0), ("taux_pour_mille", 0.0081), ("population", 12400000.0)}
    ds_json = {"resources": [{"url": "https://x/statistique-etablissements-01092026.xlsx"}, {"url": "https://x/statistique-etablissements-01082026.xlsx"},
                             {"url": "https://x/statistique-etablissements-01122025.xlsx"}, {"url": "https://x/mensuelle_inTERnet_janvier2010.xls"}]}
    assert ds.justice_latest(ds_json)["url"].endswith("01092026.xlsx")


def test_the_cnracl_employer_rate_is_known_year_by_year_from_2013_and_the_pap_has_the_four_added_missions():
    rates = {y: r for y, r, *_ in curated_pensions.CNRACL_TAUX_EMPLOYEUR}
    assert rates["2013"] == 28.85 and rates["2016"] == 30.6 and rates["2024"] == 31.65 and all(str(y) in rates for y in range(2013, 2025))
    missions = {t[1] for t in curated_pensions.PAP_CAS_PENSIONS}
    assert {"Outre-mer", "Travail et emploi"} <= missions and any(m.startswith("Agriculture") for m in missions)
    assert all(r["page"].isdigit() for r in sp.pap_rows())


def test_the_long_debt_and_population_workbooks_are_registered():
    codes = [wb.code for wb in ds.WORKBOOKS]
    assert len(codes) == len(set(codes))
    debt = next(wb for wb in ds.WORKBOOKS if wb.code == "insee_dette_longue")
    pop = next(wb for wb in ds.WORKBOOKS if wb.code == "insee_population_longue")
    assert debt.url.endswith("/fichier/2830192/econ-fin-pub-dette-adm-pub.xlsx") and debt.page.endswith("/2830192")
    assert pop.sheets == ("FR", "FM")  # the population workbook has other tabs that are not wanted


def test_a_debt_series_laid_out_like_insees_is_read_by_year():
    content = _wb({"Figure 2": [["Titre"], [], ["en % du PIB"], ["Administrations publiques", 1978, 1979, 2025], ["Ensemble ", 21.6, 21.6, 115.7]]})
    cells, texts = ds.grid_rows(content, ("Figure 2",))
    assert {(c["col"], c["value"]) for c in cells if c["row"] == "5"} == {("1", 21.6), ("2", 21.6), ("3", 115.7)}
    assert {c["value"] for c in cells if c["row"] == "4"} == {1978.0, 1979.0, 2025.0}
