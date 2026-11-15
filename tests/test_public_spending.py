"""Public spending inputs: the State budget by programme, the CAS Pensions revenue lines and the
DREES pensions by scheme parse into ledger rows; an alias keeps two queries on one Eurostat
dataset apart; a source that fails keeps its previous block, marked stale."""

import json

from dagster import build_asset_context

from homelab_datalake import layout
from homelab_datalake.assets import official_statistics as asset
from homelab_datalake.parsing import official_statistics as stats
from homelab_datalake.parsing import public_spending as sp
from homelab_datalake.resources import LakeStorage

BUDGET_2025 = (
    "type_mission;mission;code_mission;programme;libelle_programme;action;libelle_action;sous_action;libelle_sousaction;"
    "categorie;code_titre;ae_plf;cp_plf;ae_prev_fdc_adp;cp_prev_fdc_adp;ministere\n"
    "BG;Enseignement scolaire;CE;141.0;Second degré;01;x;;;21.0;2;1;100.0;0;0;Éducation\n"
    "BG;Enseignement scolaire;CE;141.0;Second degré;02;x;;;21.0;2;1;50.0;0;0;Éducation\n"
    "BG;Enseignement scolaire;CE;141.0;Second degré;02;x;;;22.0;2;1;70.0;0;0;Éducation\n"
    "BG;Enseignement scolaire;CE;141.0;Second degré;03;x;;;31.0;3;1;999.0;0;0;Éducation\n"
    "CAS;Pensions;YD;741.0;Pensions civiles;01;x;;;22.0;2;1;5.0;0;0;Budget\n"
)
EDITION_2025 = sp.BUDGET_EDITIONS[-1]

CAS_2025 = (
    "type_de_recettes;annee;libelle_mission;code_section;libelle_section;code_ligne_de_recette;libelle_ligne_de_recette;"
    "montant_recettes_plf_2025\n"
    "Recettes CAS;2025;Pensions;YD-1;Pensions civiles;YD-1-21;Personnels civils : contributions des employeurs;35077620585\n"
    "Recettes CAS;2025;Pensions;YD-2;Ouvriers;YD-2-71;Cotisations salariales et patronales;1000\n"
)

CPS = (
    "annee;ps_niveau;ps_code;ps_lib;risque;regime_nom;si_niveau;si_code;si_nom;val\n"
    "2024;1;E11-2;Vieillesse-survie;VIEILLESSE-SURVIE;Total S13111;1;S13111;État;66556,1\n"
    "2024;1;E11-1;Santé;SANTÉ;Total S13111;1;S13111;État;1000\n"
    "2023;1;E11-2;Vieillesse-survie;VIEILLESSE-SURVIE;Total S13111;1;S13111;État;63025\n"
    "2023;1;E11-2;Vieillesse-survie;VIEILLESSE-SURVIE;Total S1311;1;S1311;APUC;\n"
)


def test_budget_keeps_personnel_categories_and_sums_the_actions():
    rows = sp.budget_rows(BUDGET_2025, EDITION_2025)
    assert {(r["programme"], r["category"], r["value"]) for r in rows} == {
        ("141", "21", 150.0), ("141", "22", 70.0), ("741", "22", 5.0)}
    assert {r["type"] for r in rows} == {"BG", "CAS"}
    block = sp.budget_block(rows, [EDITION_2025], "now")
    assert block["dims"][-1] == "time" and ["BG", "Enseignement scolaire", "141", "Second degré", "21", "2025", 150.0] in block["rows"]


def test_budget_2019_labels_the_type_of_budget_in_words():
    assert sp.mission_type("Budget général") == "BG"
    assert sp.mission_type("Compte d’affectation spéciale") == "CAS"


def test_a_budget_file_that_lost_a_column_is_refused():
    broken = BUDGET_2025.replace("cp_plf", "credits")
    try:
        sp.budget_rows(broken, EDITION_2025)
    except ValueError as error:
        assert "cp_plf" in str(error)
    else:
        raise AssertionError("a missing column must stop the run")


def test_cas_keeps_the_pension_section_only():
    rows = sp.cas_rows(CAS_2025, sp.CAS_EDITIONS[-1])
    assert rows == [{"time": "2025", "line": "YD-1-21", "label": "Personnels civils : contributions des employeurs",
                     "value": 35077620585.0}]


def test_drees_keeps_old_age_and_survivors_with_decimal_commas():
    rows = sp.cps_rows(CPS)
    assert [(r["time"], r["value"]) for r in rows] == [("2024", 66556.1), ("2023", 63025.0)]
    assert sp.cps_block(rows, "now", None)["dims"][-1] == "time"


def test_ledger_rows_are_long_and_france():
    rows = sp.cas_rows(CAS_2025, sp.CAS_EDITIONS[-1])
    ledger = sp.ledger_rows("budget", sp.CAS_CODE, rows, ["line", "label", "time"], "u", "l", None)
    assert ledger[0]["geo"] == "FR" and ledger[0]["period"] == "2025" and ledger[0]["series_key"].startswith("line=YD-1-21;label=")


def test_an_alias_keeps_two_queries_on_one_eurostat_dataset_apart():
    keys = [q.key for q in stats.EUROSTAT_QUERIES]
    assert len(set(keys)) == len(keys)
    exp = next(q for q in stats.EUROSTAT_QUERIES if q.key == "gov_10a_exp_fr")
    assert exp.code == "gov_10a_exp" and "/gov_10a_exp?" in exp.url() and "geo=FR" in exp.url() and "geo=DE" not in exp.url()
    assert "sector=S1311" in exp.url() and "cofog99" not in exp.url()  # every function and sub-function


def test_collect_spending_is_all_or_nothing_per_source(monkeypatch):
    class Log:
        def warning(self, *a): pass

    def fake(url):
        if "plf25-depenses" in url:
            raise RuntimeError("down")
        if "recettes" in url:
            return CAS_2025
        if "305_" in url:
            return CPS
        return BUDGET_2025
    monkeypatch.setattr(asset, "fetch_text", fake)
    monkeypatch.setattr(asset, "collect_pension_pages", lambda *a: [])
    monkeypatch.setattr(sp, "BUDGET_EDITIONS", (EDITION_2025,))
    monkeypatch.setattr(sp, "CAS_EDITIONS", (sp.CAS_EDITIONS[-1],))
    rows, blocks, failed = asset.collect_spending("now", Log())
    assert failed == ["budget:state_budget_personnel"]
    assert "drees:cps_vieillesse_survie" in blocks and "budget:state_budget_personnel" not in blocks


def test_the_asset_lands_the_spending_blocks_and_exports_them(tmp_path, monkeypatch):
    payload = {"id": ["freq", "geo", "time"], "size": [1, 1, 1], "label": "x", "updated": "2026-10-21",
               "dimension": {"freq": {"category": {"index": {"A": 0}}}, "geo": {"category": {"index": {"FR": 0}}},
                             "time": {"category": {"index": {"2024": 0}}}}, "value": {"0": 1}}
    monkeypatch.setattr(stats, "EUROSTAT_QUERIES", (stats.EurostatQuery("gov_10a_exp", {}, "1995", "t", alias="gov_10a_exp_fr"),))
    monkeypatch.setattr(asset, "fetch_eurostat", lambda q: payload)
    monkeypatch.setattr(asset, "fetch_ssmsi", lambda: ([], {"title": "DEP", "last_modified": "2026-07-09", "url": "u"}))

    def fake(url):
        if "recettes" in url:
            return CAS_2025
        return CPS if "305_" in url else BUDGET_2025
    monkeypatch.setattr(asset, "fetch_text", fake)
    monkeypatch.setattr(asset, "collect_deeper", lambda fetched_at, log: ([], {}, []))
    monkeypatch.setattr(asset, "collect_crime", lambda fetched_at, log: ([], {}, []))
    monkeypatch.setattr(asset, "collect_pension_pages", lambda *a: [])
    monkeypatch.setattr(sp, "BUDGET_EDITIONS", (EDITION_2025,))
    monkeypatch.setattr(sp, "CAS_EDITIONS", (sp.CAS_EDITIONS[-1],))
    export_dir = tmp_path / "export"
    export_dir.mkdir()
    monkeypatch.setenv("OFFICIAL_STATS_EXPORT_DIR", str(export_dir))
    lake = LakeStorage(local_root=str(tmp_path / "lake"))
    result = asset.official_statistics_raw(build_asset_context(), lake)
    assert all(check.passed for check in result.check_results)
    export = json.loads((export_dir / asset.EXPORT_NAME).read_text())
    assert {"eurostat:gov_10a_exp_fr", "budget:state_budget_personnel", "budget:cas_pensions_recettes",
            "drees:cps_vieillesse_survie"} <= set(export["datasets"])
    landed = lake.read_table(f"{layout.prefix('official_stat_observations')}/data.parquet")
    assert {"budget", "drees", "eurostat"} == set(landed.column("source").to_pylist())


# ---------------------------------------------------------------- COR, Service des retraites de l'État, DREES, curated values
import io  # noqa: E402

import openpyxl  # noqa: E402

from homelab_datalake.parsing import curated_pensions  # noqa: E402
from homelab_datalake.parsing.xlsx import read as read_xlsx  # noqa: E402


def _workbook(sheets: dict[str, list[list]]) -> bytes:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_the_xlsx_reader_returns_numbers_text_and_row_numbers():
    sheets = read_xlsx(_workbook({"A": [["titre", None], [None, "x", 1.5], ["y", "z"]]}))
    assert sheets["A"][0] == (1, ["titre"]) or sheets["A"][0][1][0] == "titre"
    row = next(r for n, r in sheets["A"] if n == 2)
    assert row[1] == "x" and row[2] == 1.5


def test_cor_blocks_keep_observed_and_projected_series_apart_and_the_unit_in_the_name():
    years = [None, None, None, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    rows = [["Titre"], [], years,
            [None, "En % du PIB", None], [None, "Prestations totales", "Obs", None, None, None, None, None, None, 13.0, 13.5, None], [None, None, "Sc. Ref", None, None, None, None, None, None, None, None, 13.7],
            [None, "En Mds€", None], years, [None, "Prestations totales", "Obs", None, None, None, None, None, None, 380.0, 400.0, None], [None, None, "Sc. Ref", None, None, None, None, None, None, None, None, 410.0]]
    out = sp.cor_block_rows(read_xlsx(_workbook({"S": rows}))["S"], "")
    keys = {(r["series"], r["scenario"], r["time"]): r["value"] for r in out}
    assert keys[("Prestations totales [En % du PIB]", "Obs", "2024")] == 13.5
    assert keys[("Prestations totales [En Mds€]", "Obs", "2023")] == 380.0
    assert keys[("Prestations totales [En Mds€]", "Sc. Ref", "2025")] == 410.0


SRE_HTML = ("<html><body><p>741 - PCMR et ATI 57 290,42 58 314,73 59 779,62 61 228,16 63 955,00 Cotisations salariales 7 120,19 7 199,54 7 355,44 7 503,80 7 529,95 "
            "dont État 5 820,45 5 921,52 6 069,84 6 234,36 6 305,09 Contributions employeurs 49 502,64 50 075,29 51 291,87 52 618,20 55 084,40 "
            "dont État 42 252,19 43 008,69 44 125,70 45 417,30 47 664,71</p><p>Équilibre Exécution budgétaire* 2021 2022 2023 2024 2025 "
            "Taux de cotisation agent 11,1 11,1 11,1 11,1 11,1 Taux de contribution employeur au titre des pensions civiles 74,28 74,28 74,28 74,28 78,28 "
            "au titre des pensions militaires 126,07 126,07 126,07 126,07 126,07</p></body></html>")


def test_the_executed_cas_page_gives_contributions_and_rates_by_year():
    keys = {(r["series"], r["time"]): r["value"] for r in sp.sre_rows(SRE_HTML)}
    assert keys[("contributions_employeurs", "2023")] == 51291.87 and keys[("contributions_employeurs_etat", "2025")] == 47664.71
    assert keys[("taux_contribution_civils_pct", "2025")] == 78.28 and keys[("taux_contribution_militaires_pct", "2021")] == 126.07


def test_a_page_that_lost_its_tables_is_refused():
    try:
        sp.sre_rows("<html><body>plus de tableaux</body></html>")
    except ValueError as error:
        assert "no longer" in str(error)
    else:
        raise AssertionError


def test_drees_workbook_resources_by_year_and_the_resource_picked_from_data_gouv():
    wb = _workbook({"opérations_1959_2024": [[None, "1995R", "2024SD"], ["Cotisations sociales d'employeurs", 137.5, 294.9], ["* note", None, None]]})
    keys = {(r["series"], r["time"]): (r["value"], r["scenario"]) for r in sp.cps_ressources_rows(wb)}
    assert keys[("Cotisations sociales d'employeurs", "1995")] == (137.5, "R") and keys[("Cotisations sociales d'employeurs", "2024")][1] == "SD"
    url, _ = sp.cps_workbook_url({"resources": [
        {"title": "La protection sociale en France de 1959 à 2023.xlsx", "format": "vnd.openxmlformats-officedocument.spreadsheetml.sheet", "url": "old"},
        {"title": "La protection sociale en France de 1959 à 2024.xlsx", "format": "vnd.openxmlformats-officedocument.spreadsheetml.sheet", "url": "new", "last_modified": "2026-10-23"},
        {"title": "x.csv", "format": "csv", "url": "csv"}]})
    assert url == "new"


def test_curated_values_carry_their_page_and_the_programmes_add_up():
    pap = sp.pap_rows()
    assert all(r["page"].isdigit() for r in pap)
    ens = [r for r in pap if r["edition"] == "PAP2026" and r["mission"] == "Enseignement scolaire" and r["time"] == "2025" and r["item"] == "contributions_equilibre_cas"]
    assert sum(r["value"] for r in ens) > 20e9  # the school programmes carry more than 20 Md€ of CAS contributions in 2025
    block = sp.ccss_block("now")
    assert ["CNRACL", "CCSS octobre 2024 (résultats 2023)", "188", "https://www.vie-publique.fr/files/rapport/pdf/299216.pdf", "2023", 17484e6] in block["rows"]
    assert all(c[5] and c[4] for c in curated_pensions.CCSS_COTISATIONS_PATRONALES)
