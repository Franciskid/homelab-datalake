"""Insee origin tables: the census département tables and the births-by-mother's-country CSV parse into rows keyed by département."""

import io
import zipfile

from homelab_datalake.parsing import insee_origins as io_


def _zip(name: str, text: str, meta: bool = True) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, text)
        if meta:
            z.writestr(f"meta_{name}", "variable;lib\n")
    return buf.getvalue()


V1 = ("NIVGEO;CODGEO;LIBGEO;ANNEE;SEXE;AGE4;PNAI12_A;NB\n"
      "DEP;01;Ain;2021;ENS;ENS;ENS;80099.5\nDEP;01;Ain;2021;ENS;ENS;07;4561.8\nDEP;01;Ain;2021;1;ENS;07;2000\n"
      "DEP;2A;Corse-du-Sud;2021;ENS;ENS;ENS;14960.9\nCOM;01001;X;2021;ENS;ENS;ENS;5\nDEP;02;Aisne;2021;ENS;ENS;ENS;\n")
V2 = ("NIVGEO;CODGEO;LIBGEO;ANNEE;SEXE;AGE4;PNAI48_A;NB\nDEP;01;Ain;2021;ENS;ENS;ENS;80099.5\nDEP;01;Ain;2021;ENS;ENS;32;900\n")
POP = ("NIVGEO;CODGEO;LIBGEO;AGE4;IMMI;SEXE;NB\nCOM;01001;A;00;1;1;10\nCOM;01001;A;00;2;1;90\nCOM;01002;B;00;2;2;50\n"
       "COM;2A004;C;00;2;1;30\nCOM;97101;D;00;2;1;5\nARM;13201;M1;00;1;1;999\nCOM;01003;E;00;2;1;\n")


def test_census_tables_become_rows_by_departement_and_the_population_is_the_sum_of_communes():
    rows = io_.immigrants_rows("2021", _zip("v1.csv", V1), _zip("v2.csv", V2), _zip("pop.csv", POP, meta=False), min_deps=1)
    got = {(r["pays"], r["geo"]): r["value"] for r in rows}
    assert got[("AENS", "01")] == 80099.5 and got[("A07", "01")] == 4561.8 and got[("B32", "01")] == 900.0
    assert ("A07", "01") in got and got[("AENS", "2A")] == 14960.9
    assert ("AENS", "02") not in got, "a blank count is absent, not zero"
    assert got[("POP", "01")] == 150.0 and got[("POP", "2A")] == 30.0 and got[("POP", "971")] == 5.0
    assert ("POP", "13") not in got, "arrondissements are not counted twice: Marseille is its commune"
    assert all(r["time"] == "2021" for r in rows)


def test_a_zip_with_no_data_csv_is_refused():
    import pytest
    with pytest.raises(ValueError):
        io_.zip_csv(_zip("meta_only.csv", "x", meta=False))
    with pytest.raises(ValueError, match="fewer than 90"):
        io_.immigrants_rows("2021", _zip("v1.csv", V1), _zip("v2.csv", V2), _zip("pop.csv", POP, meta=False))


def test_births_2024_codes_become_departements():
    text = ("REGDEP_DOMI_MERE;PNSEL_MERE;NBNAIS\n1175;FRANCE;12756\n1175;ETRANGER;8728\n942A;ETRANGER;400\n11XX;ETRANGER;99\n"
            "971;MAROC;12\nFM;TOTAL;610000\nFR;TOTAL;659731\n7516;ALGERIE;x\n99XX;FRANCE;1\n")
    rows = io_.births_2024_rows(text)
    got = {(r["pays_mere"], r["geo"]): r["value"] for r in rows}
    assert got[("FRANCE", "75")] == 12756 and got[("ETRANGER", "2A")] == 400 and got[("MAROC", "971")] == 12
    assert got[("TOTAL", "FM")] == 610000 and ("ETRANGER", "11") not in got and not any(r["geo"].endswith("XX") for r in rows)
    assert len(rows) == 6


def test_the_new_workbooks_are_pinned_and_unique():
    from homelab_datalake.parsing import deeper_series as d
    codes = [w.code for w in d.WORKBOOKS]
    assert len(codes) == len(set(codes))
    for c in ("insee_immigres_pays_1968_2019", "insee_immigres_groupes_2006_2025", "insee_naissances_pays_mere_nat", "insee_naissances_dep_pays_mere_2025"):
        assert c in codes
