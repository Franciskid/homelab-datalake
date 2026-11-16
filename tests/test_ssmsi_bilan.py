"""The SSMSI bilan: the series workbooks, the checks against the synthèse and the PDF, the manual inputs, and the once-a-year plain request."""

# ruff: noqa: RUF001 -- the typographic quotes and dashes are the publisher's

import io
import os
import zipfile
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from homelab_datalake import manual_inputs as manual
from homelab_datalake.assets import ssmsi_bilan as asset
from homelab_datalake.parsing import ssmsi_bilan as s
from homelab_datalake.resources import LakeStorage

YEARS = list(range(2016, 2026))


def series_workbook(total=1000, french=800, masked=None, unknown=5) -> tuple[list, list]:
    """The `natio` and `natio_majmin` sheets of a series file, for every indicator: the name only in the 2016 column, a code in the others."""
    head = ["Nationalité"] + [None] * 0
    years = [None]
    for slug, label, _stem in s.INDICATORS:
        for k, y in enumerate(YEARS):
            head.append(label if k == 0 else f"indic_{slug}")
            years.append(float(y))
    groups = {"France": french, "Maghreb": 60, "Afrique (hors Maghreb)": 40, "Asie": 20, "Amérique": 20, "Océanie": 5, "Espagnols, Italiens, Portugais": 20,
              "Autres nationalités de l'Union européenne à 27": 20, "Europe hors UE27": 10, "Non renseigné": unknown}
    natio = [(1, head), (2, years)]
    for i, (g, v) in enumerate(groups.items()):
        cells = [g] + [("secr." if (masked and g == masked) else float(v)) for _ in head[1:]]
        natio.append((3 + i, cells))
    mm_head = ["Nationalité", "Majorité", *head[1:]]
    mm_years = [None, None, *years[1:]]
    rows = [(1, mm_head), (2, mm_years)]
    for i, (a, b) in enumerate([("Française", "Majeur"), (None, "Mineur"), ("Étrangère", "Majeur"), ("Etrangère", "Mineur")]):
        rows.append((3 + i, [a, b] + [1.0] * len(head[1:])))
    rows.append((7, ["Total", "Majeur"] + [float(total - 100)] * len(head[1:])))
    rows.append((8, ["Etrangère", "Mineur"] + [100.0] * len(head[1:])))  # the second total line is mislabelled in the real file
    return natio, rows


def test_series_rows_read_groups_and_totals_and_skip_masked_cells():
    natio, majmin = series_workbook(masked="Asie")
    rows = s.series_rows(natio, majmin)
    got = {(r["indicator"], r["group"], r["time"]): r["value"] for r in rows}
    assert got[("homicides", "FR", "2016")] == 800 and got[("cambriolages", "TOTAL", "2025")] == 1000 and got[("escroqueries", "MAGHREB", "2020")] == 60
    assert ("homicides", "ASIA", "2016") not in got            # « secr. » is left out, not read as zero
    assert {r["indicator"] for r in rows} == {slug for slug, _l, _st in s.INDICATORS} and len({r["time"] for r in rows}) == 10
    assert s.check_series(rows, kind="mec")["totals_checked"] == 180


def test_a_year_block_without_a_name_is_refused():
    natio, majmin = series_workbook()
    natio[0][1][1] = "Libellé inconnu"
    with pytest.raises(ValueError, match="not a known indicator"):
        s.series_rows(natio, majmin)


def test_groups_larger_than_the_total_fail_the_block():
    natio, majmin = series_workbook(total=500)   # 800 French people for 500 in total
    with pytest.raises(ValueError, match="exceed the total"):
        s.check_series(s.series_rows(natio, majmin), kind="mec")


def test_series_are_checked_against_the_synthese_to_a_twentieth_of_a_point():
    natio, majmin = series_workbook()
    rows = s.series_rows(natio, majmin)
    share = 100 * (1000 - 800) / 1000
    profile = {slug: share for slug, _l, _st in s.INDICATORS}
    changes = {slug: {"victims": share, "victims_change": 0.0, "charged": share, "charged_change": 0.0} for slug, _l, _st in s.INDICATORS}
    assert s.check_against_synthese(rows, rows, profile, changes)["max_gap_points"] == 0
    profile["cambriolages"] = share + 0.5
    with pytest.raises(ValueError, match="cambriolages"):
        s.check_against_synthese(rows, rows, profile, changes)


def test_pdf_profile_reads_the_foreigners_column_and_the_population_share():
    text = """Figure 7 > Profil des mis en cause enregistrés
                         Femmes    Étrangers
                         Homicides                                  9            43              28                                  18
            Tentatives d’homicide                                   12           49              26                                  20
             Violences physiques                                    10          28              39                                   16
          dont violences physiques
                hors cadre familial                                  21         31               26                               15
                Vols sans violence                                                                   30
            contre des personnes                                    15          32                                              29
Part dans la population en France           51 %           14 %      6%       14 %     19 %      19 %     28 %                        9%
"""
    printed = s.pdf_profile([text])
    assert printed["foreign"] == {"homicides": 18, "tentatives_homicide": 20, "violences_hors_famille": 15, "vols_sans_violence": 29}
    assert s.population_stated(printed) == 9
    with pytest.raises(s.PdfLayoutError):
        s.pdf_profile(["nothing here"])


def test_pdf_disagreement_fails_and_rounding_does_not():
    printed = {"page": 19, "foreign": {f"i{k}": 18.0 for k in range(9)}, "population_pct": [9.0]}
    ok = {f"i{k}": 18.4 for k in range(9)}
    assert s.check_against_pdf(ok, printed)["compared_with_pdf"] == 9
    with pytest.raises(ValueError, match="prints 18 %"):
        s.check_against_pdf({**ok, "i3": 19.2}, printed)
    with pytest.raises(s.PdfLayoutError):
        s.check_against_pdf({"i0": 18.0}, printed)


def test_group_population_shares_come_from_the_labels():
    sheet = [(1, ["Nationalité", None]), (2, ["Français (91 %)", 1092]), (3, ["Etrangers (9 %) :", 242]), (4, ["Maghreb (Algériens, Marocains, Tunisiens) [2 %]", 73]),
             (5, ["Autres nationalités d'Afrique [2 %]", 51]), (6, ["Amérique (<1 %)", 22]), (7, ["Non renseignée", 1])]
    assert s.group_population_shares(sheet) == {"FR": 91, "FOREIGN": 9, "MAGHREB": 2, "AFRICA_OTHER": 2}


def zip_of(files: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    return out.getvalue()


def test_zip_members_skip_excel_lock_files_and_find_one_file():
    data = zip_of({"a/Mec_2016_2025 - x.xlsx": b"1", "a/~$Mec_2016_2025 - x.xlsx": b"lock", "b/Victimes_2016_2025.xlsx": b"2", "c/notes.txt": b"t"})
    members = s.zip_members(data)
    assert set(members) == {"a/Mec_2016_2025 - x.xlsx", "b/Victimes_2016_2025.xlsx"}
    assert s.find(members, "mec 2016") == b"1"
    with pytest.raises(ValueError):
        s.find(members, "2016")


def ir_workbook() -> bytes:
    wb = openpyxl.Workbook()
    for name, total, charged in (("Figure 1", 284681, False), ("Figure 6", 217857, True)):
        ws = wb.create_sheet(name)
        ws.append([name])
        ws.append([None, "n", "rep", "femmes", "françaises", "2024", "evol"])
        ws.append([None, "Nombre", "Répartition"])
        ws.append(["Total", total, 100, 84, 84 if charged else 86, total - 100, 5])
        ws.append(["Homicide", 124, "<1", 78, 89, 134, -7])
        ws.append(["Violences physiques et administration de substances nuisibles", 179542, 63, 82, 84, 173155, 4])
    del wb["Sheet"]
    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


def test_ir69_reads_counts_french_share_and_the_restated_previous_year():
    got = {(r["side"], r["category"], r["time"], r["measure"]): r["value"] for r in s.ir69_rows(ir_workbook())}
    assert got[("victims", "total", "2025", "n")] == 284681 and got[("victims", "total", "2025", "french_pct")] == 86
    assert got[("charged", "total", "2025", "french_pct")] == 84 and got[("victims", "total", "2024", "n_revised")] == 284581
    assert got[("victims", "homicide", "2025", "french_pct")] == 89


IR56_PAGE = """Figure 1 – Victimes de violences conjugales enregistrées par la police et la gendarmerie en 2024
Total                                            272 382                100                   84                   86               271 263          0
Actes entraînant ou visant
                                                       534                <1                  69                   84                   552          -3
à entraîner la mort
   Homicide                                            134                <1                  76                   89                    115        16
Violences physiques
et administration                                 173 163                64                   83                   84               173 308          0
de substances nuisibles
Violences sexuelles                                13 508                  5                  98                   87                 12 107        12
Violences verbales
                                                   85 177                 31                  85                   89                85 296          0
ou psychologiques
"""
IR56_CHARGED = IR56_PAGE.replace("Figure 1 – Victimes de violences conjugales", "Figure 6 – Mis en cause par les services de sécurité en 2024 ").replace("272 382", "214 469")


def test_ir56_reads_the_names_that_run_over_several_lines():
    got = {(r["side"], r["category"], r["measure"]): r["value"] for r in s.ir56_rows([IR56_PAGE, IR56_CHARGED])}
    assert got[("victims", "total", "n")] == 272382 and got[("victims", "homicide", "french_pct")] == 89 and got[("victims", "physiques", "n")] == 173163
    assert got[("victims", "psychologiques", "n")] == 85177 and got[("victims", "sexuelles", "french_pct")] == 87 and got[("charged", "total", "n")] == 214469


# ---------------------------------------------------------------- manual inputs

def test_a_manual_input_is_stored_with_its_provenance_and_read_back_verified(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    e = manual.put(lake, "ssmsi", "bilan_data_zip", "x.zip", b"zipbytes", edition=2026, url="https://example.org/x.zip", how="manual: browser", obtained_on="2026-11-16")
    assert manual.put(lake, "ssmsi", "bilan_data_zip", "x.zip", b"zipbytes", edition=2026, url="u", how="h") == e     # the same bytes are recorded once
    assert len(manual.read_manifest(lake, "ssmsi")["entries"]) == 1
    data, entry = manual.get(lake, "ssmsi", "bilan_data_zip")
    assert data == b"zipbytes" and entry["sha256"] == manual.sha256(b"zipbytes") and entry["how"] == "manual: browser"
    manual.put(lake, "ssmsi", "bilan_data_zip", "y.zip", b"newer", edition=2027, url="u", how="h")
    assert manual.get(lake, "ssmsi", "bilan_data_zip")[0] == b"newer" and manual.get(lake, "ssmsi", "bilan_data_zip", 2026)[0] == b"zipbytes"
    (Path(tmp_path) / e["key"]).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="sha256"):
        manual.get(lake, "ssmsi", "bilan_data_zip", 2026)
    with pytest.raises(FileNotFoundError):
        manual.get(lake, "ssmsi", "nothing")


class Log:
    def __init__(self):
        self.warnings, self.infos = [], []

    def warning(self, message, *args):
        self.warnings.append(message % args)

    def info(self, message, *args):
        self.infos.append(message % args)


def stored_edition(lake, edition=2026):
    manual.put(lake, "ssmsi", "bilan_pdf", "old.pdf", b"%PDF-old", edition=edition, url="u", how="fetched", obtained_on="2026-07-10")


def test_the_bilan_pdf_url_is_the_one_the_host_uses():
    assert asset.bilan_pdf_url(2026, 7).endswith("/ssmsi/2026-07/Ins%C3%A9curit%C3%A9%20et%20d%C3%A9linquance%20en%202025%20-%20Bilan%20statistique%20-%20%C3%A9dition%202026%20-%20SSMSI.pdf")


def test_no_request_before_the_next_edition_is_due_and_one_a_week_after(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    stored_edition(lake)
    asked = []

    def get(url):
        asked.append(url)
        return 404, b""

    assert asset.probe_next_edition(lake, Log(), date(2026, 11, 16), get, pause=0) is None and asked == []          # 2027's edition is due in June 2027
    assert asset.probe_next_edition(lake, Log(), date(2027, 6, 20), get, pause=0) is None
    assert [u.split("/ssmsi/files/ssmsi/")[1].split("/")[0] for u in asked] == ["2027-06"]                       # June only: the later months are not yet
    asked.clear()
    assert asset.probe_next_edition(lake, Log(), date(2027, 6, 24), get, pause=0) is None and asked == []          # asked three days ago
    assert asset.probe_next_edition(lake, Log(), date(2027, 9, 10), get, pause=0) is None
    assert [u.split("/ssmsi/files/ssmsi/")[1].split("/")[0] for u in asked] == ["2027-06", "2027-07", "2027-08", "2027-09"]


def test_a_new_edition_found_by_a_plain_request_is_stored(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    stored_edition(lake)

    def get(url):
        return (200, b"%PDF-1.7 new edition") if "2027-07" in url else (404, b"")

    log = Log()
    entry = asset.probe_next_edition(lake, log, date(2027, 7, 15), get, pause=0)
    assert entry["edition"] == 2027 and entry["how"].startswith("fetched: plain request") and manual.get(lake, "ssmsi", "bilan_pdf")[0].startswith(b"%PDF-1.7")
    assert log.infos


@pytest.mark.parametrize("status", [403, 503, 0])
def test_a_refusal_is_logged_and_the_stored_copy_is_kept_without_another_try(tmp_path, status):
    lake = LakeStorage(local_root=str(tmp_path))
    stored_edition(lake)
    asked = []

    def get(url):
        asked.append(url)
        return status, b""

    log = Log()
    assert asset.probe_next_edition(lake, log, date(2027, 9, 10), get, pause=0) is None
    assert len(asked) == 1 and "kept" in log.warnings[0] and manual.get(lake, "ssmsi", "bilan_pdf")[0] == b"%PDF-old"


def test_a_page_that_is_not_a_pdf_is_not_stored(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    stored_edition(lake)
    log = Log()
    assert asset.probe_next_edition(lake, log, date(2027, 7, 15), lambda url: (200, b"<html>blocked</html>"), pause=0) is None
    assert manual.latest(lake, "ssmsi", "bilan_pdf")["edition"] == 2026


# ---------------------------------------------------------------- the real files, when they are on this machine

SOURCES = Path(os.environ.get("SSMSI_SOURCES", Path.home() / "dev/politico-sources/ssmsi"))
real = pytest.mark.skipif(not (SOURCES / "SSMSI-bilan-2025-donnees.zip").exists(), reason="the SSMSI files are not on this machine")


@real
def test_the_real_tables_agree_with_the_synthese_and_the_pdf():
    tables = s.bilan_tables((SOURCES / "SSMSI-bilan-2025-donnees.zip").read_bytes())
    assert tables["checks"]["synthese"]["max_gap_points"] == 0 and tables["checks"]["synthese"]["compared_with_synthese"] >= 60
    printed = s.pdf_profile(s.pdf_text((SOURCES / "SSMSI-bilan-2025-edition-2026.pdf").read_bytes()))
    assert printed["page"] == 19 and s.population_stated(printed) == 9
    assert s.check_against_pdf(tables["profile"], printed)["compared_with_pdf"] == 17
    by = {(r["indicator"], r["group"], r["time"]): r["value"] for r in tables["mec"]}
    assert by[("homicides", "TOTAL", "2025")] == 1334 and by[("homicides", "FR", "2025")] == 1092 and by[("vols_dans_vehicules", "TOTAL", "2025")] > 10000


@real
def test_the_real_domestic_violence_files():
    new = {(r["side"], r["category"], r["measure"]): r["value"] for r in s.ir69_rows((SOURCES / "IR69-donnees-violences-conjugales-2025.xlsx").read_bytes())}
    old = {(r["side"], r["category"], r["measure"]): r["value"] for r in s.ir56_rows(s.pdf_text((SOURCES / "IR56-violences-conjugales-2024.pdf").read_bytes()))}
    assert new[("victims", "total", "n")] == 284681 and new[("charged", "total", "french_pct")] == 84 and old[("victims", "total", "n")] == 272382
    assert abs(new[("victims", "total", "n_revised")] - old[("victims", "total", "n")]) < 0.001 * old[("victims", "total", "n")]


@real
def test_the_collector_lands_the_blocks_from_the_stored_files(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    for role, edition, name in (("bilan_data_zip", 2026, "SSMSI-bilan-2025-donnees.zip"), ("bilan_pdf", 2026, "SSMSI-bilan-2025-edition-2026.pdf"),
                                ("ir_domestic_pdf", 2024, "IR56-violences-conjugales-2024.pdf"), ("ir_domestic_pdf", 2025, "IR69-violences-conjugales-2025.pdf"),
                                ("ir_domestic_xlsx", 2025, "IR69-donnees-violences-conjugales-2025.xlsx")):
        manual.put(lake, "ssmsi", role, name, (SOURCES / name).read_bytes(), edition=edition, url="https://example.org/" + name, how="manual: test")
    asked = []
    rows, blocks, failed = asset.collect_ssmsi_bilan("2026-11-16T00:00:00+00:00", Log(), lake, get=lambda url: asked.append(url) or (404, b""), today=date(2026, 11, 16))
    assert failed == [] and set(blocks) == {"crime:fr_mec_nat", "crime:fr_vic_nat", "crime:fr_conjugal"} and asked == []
    b = blocks["crime:fr_mec_nat"]
    assert b["dims"] == ["indicator", "group", "time"] and b["population_foreign_pct_stated"] == 9 and b["checks"]["pdf"]["compared_with_pdf"] == 17
    assert b["inputs"][0]["sha256"] == manual.sha256((SOURCES / "SSMSI-bilan-2025-donnees.zip").read_bytes())
    assert len(rows) == len(blocks["crime:fr_mec_nat"]["rows"]) + len(blocks["crime:fr_vic_nat"]["rows"]) + len(blocks["crime:fr_conjugal"]["rows"])


def test_a_missing_manual_input_fails_the_block_and_names_it(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    log = Log()
    rows, blocks, failed = asset.collect_ssmsi_bilan("2026-11-16T00:00:00+00:00", log, lake, get=lambda url: (404, b""), today=date(2026, 11, 16))
    assert rows == [] and blocks == {} and failed == ["crime:fr_bilan", "crime:fr_conjugal"]
    assert any("no manual input" in w for w in log.warnings)
