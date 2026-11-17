"""Indicators by département: long Insee tables become shares by département code; a file that changed shape is refused."""

import io
import json
import zipfile
from pathlib import Path

import pytest

from homelab_datalake.assets import department_indicators as asset
from homelab_datalake.parsing import department_indicators as di


def zipped(text: str, name: str = "DS_X_data.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, text)
    return buf.getvalue()


def test_diplomes_are_shares_of_the_non_schooled_population_of_the_departement():
    rows = ['"GEO";"GEO_OBJECT";"AGE";"SEX";"EDUC";"RP_MEASURE";"FREQ";"OBS_STATUS";"TIME_PERIOD";"OBS_VALUE"']
    for dep, tot, low, high in (("01", 1000, 300, 200), ("2A", 400, 80, 100)):
        for educ, v in (("_T", tot), ("001T100_RP", low), ("500T702_RP", high), ("300_RP", 7)):
            rows.append(f'"{dep}";"DEP";"Y_GE15";"_T";"{educ}";"POP";"A";"A";2023;{v}')
    rows.append('"01";"DEP";"Y_GE15";"_T";"_T";"POP";"A";"A";2017;5')  # another year
    rows.append('"01";"COM";"Y_GE15";"_T";"_T";"POP";"A";"A";2023;9')  # another geography
    out = di.diplomes(zipped("\n".join(rows)))
    assert out["sans_diplome"]["values"] == {"01": 30.0, "2A": 20.0} and out["diplome_superieur"]["values"] == {"01": 20.0, "2A": 25.0}


def test_unemployment_and_overcrowding():
    emp = ['"GEO";"GEO_OBJECT";"SEX";"EMPSTA_ENQ";"AGE";"EDUC";"RP_MEASURE";"FREQ";"OBS_STATUS";"TIME_PERIOD";"OBS_VALUE"',
           '"75";"DEP";"_T";"1T2";"Y15T64";"_T";"POP";"A";"A";2023;1000', '"75";"DEP";"_T";"2";"Y15T64";"_T";"POP";"A";"A";2023;100',
           '"75";"DEP";"_T";"1";"Y15T64";"_T";"POP";"A";"A";2023;900']
    assert di.emploi(zipped("\n".join(emp)))["chomage_rp"]["values"] == {"75": 10.0}
    log = ['"GEO";"GEO_OBJECT";"OCS";"OCC_IND";"RP_MEASURE";"FREQ";"OBS_STATUS";"TIME_PERIOD";"OBS_VALUE"']
    for occ, v in (("_T", 1000), ("MOD_OVER_OCC", 100), ("SEV_OVER_OCC", 50), ("STD_OCC", 500)):
        log.append(f'"93";"DEP";"DW_MAIN";"{occ}";"DWELLINGS";"A";"A";2023;{v}')
    assert di.logement(zipped("\n".join(log)))["suroccupation"]["values"] == {"93": 15.0}


def test_a_table_that_lost_a_column_is_refused():
    with pytest.raises(ValueError):
        di.diplomes(zipped('"GEO";"GEO_OBJECT";"AGE"\n"01";"DEP";"Y_GE15"'))


def test_the_export_is_written_whole_and_a_thin_indicator_is_refused(tmp_path: Path, monkeypatch):
    fake = {k: {"values": {f"{i:02d}": float(i) for i in range(1, 96)}, "year": "2023", "unit": "%", "direction": "higher_is_more", "label": k, "definition": k} for k in ("a", "b")}
    monkeypatch.setattr(di, "PARSERS", {k: (lambda data, k=k: dict(fake)) for k in di.SOURCES})
    doc = asset.build(get=lambda url: b"x")
    path = asset.write(tmp_path, doc)
    assert json.loads(path.read_text())["indicators"]["a"]["source"] in di.SOURCES and not any(p.name.startswith(".") for p in tmp_path.iterdir())
    monkeypatch.setattr(di, "PARSERS", {k: (lambda data: {"x": {"values": {"01": 1.0}, "year": "2023", "unit": "", "direction": "", "label": "", "definition": ""}}) for k in di.SOURCES})
    with pytest.raises(ValueError, match="départements only"):
        asset.build(get=lambda url: b"x")
