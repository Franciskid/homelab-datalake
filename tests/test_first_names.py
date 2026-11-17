"""The Fichier des prénoms is found from the series page, cut into per-letter files, and exported without ever leaving half of them."""

import io
import json
import zipfile
from pathlib import Path

from homelab_datalake.assets import first_names as asset
from homelab_datalake.parsing import first_names as fn

NAT = ("sexe;prenom;periode;valeur;rang\n"
       "2;MARIE;1900;100;1\n2;MARIE;1901;105;1\n2;MARIE;1903;95;1\n1;JEAN;1900;60;1\n1;JEAN;1901;55;1\n1;ÉMILE;1901;10;2\n2;ÉLODIE;2025;25;3\n"
       "1;JEAN;2025;5;9\n2;MARIE;2025;5;9\n1;ZED;2025;5;10\n")
DPT = ("sexe;prenom;periode;dpt;valeur;rang\n"
       "2;MARIE;1900;01;100;1\n2;MARIE;1901;75;100;1\n2;MARIE;1903;2A;95;1\n2;MARIE;2025;971;5;3\n1;JEAN;1900;01;60;1\n1;JEAN;1901;01;55;1\n1;ÉMILE;1901;01;10;2\n")


def zipped(text: str, name: str = "x.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, text)
    return buf.getvalue()


def test_a_name_goes_to_the_file_of_its_unaccented_first_letter():
    assert fn.letter("ÉMILE") == "E" and fn.letter("ZED") == "Z" and fn.letter("7UP") == "_" and fn.letter("ÇA") == "C"


def test_the_edition_is_read_from_the_page_that_links_both_files():
    html = '<a href="/fr/statistiques/fichier/8595130/prenoms-2025-nat_csv.zip"></a><a href="/fr/statistiques/fichier/8595130/prenoms-2025-dpt_csv.zip"></a>'
    got = fn.edition_links(html)
    assert got["edition"] == 2025 and got["nat"].endswith("prenoms-2025-nat_csv.zip") and got["dpt"].startswith("https://www.insee.fr/")
    assert fn.edition_links('<a href="/fr/statistiques/fichier/1/prenoms-2024-nat_csv.zip"></a>') is None
    assert fn.edition_links("<p>another page</p>") is None


def test_the_cut_keeps_counts_by_year_by_decade_and_the_denominators():
    files = fn.build(fn.read_nat(NAT), fn.read_dpt(DPT), 2025, {"page": "p"})
    m = files["manifest.json"]
    assert m["years"] == [1900, 2025] and m["rounding"] == 5 and m["deps"] == ["01", "2A", "75", "971"]
    marie = next(n for n in files["nat-M.json"]["names"] if n[0] == "MARIE")
    assert marie[1] == 2 and marie[2] == 1900 and marie[3][:4] == [20, 21, 0, 19], "counts/5 from the first year, zero in between"
    assert marie[3][-1] == 1 and len(marie[3]) == 126
    assert [n[0] for n in files["nat-E.json"]["names"]] == ["ÉLODIE", "ÉMILE"], "by volume, accented names in the letter of their base"
    assert m["births"]["2"][0] == 100 and m["births"]["1"][125] == 10, "published births per year and sex"
    assert files["tops.json"]["2025"]["1"] == [["JEAN", 5], ["ZED", 5]], "ties by name"
    births = files["dep-births.json"]["2"]
    assert births[0][0] == 100 and births[2][0] == 100 and births[3][12] == 5, "births per sex, département and decade"


def test_a_map_needs_enough_births():
    files = fn.build(fn.read_nat(NAT), fn.read_dpt(DPT), 2025, {})
    marie = next(n for n in files["dep-M.json"]["names"] if n[0] == "MARIE")
    assert marie[2] == [0, 0, 20, 0, 1, 19, 0, 2, 20, 12, 3, 1], "decade, département index, counts/5"
    assert "dep-E.json" not in files and "dep-J.json" not in files, "ÉMILE (10) and JEAN (115) are under the 200 births a map needs"


def test_a_national_file_that_stops_early_is_refused():
    import pytest
    nat = fn.read_nat("sexe;prenom;periode;valeur;rang\n1;JEAN;1900;60;1\n")
    with pytest.raises(ValueError):
        fn.build(nat, [], 2025, {})


def test_the_directory_is_swapped_whole_and_a_known_edition_is_not_fetched_again(tmp_path: Path):
    class Log:
        def info(self, *a): pass
    found = {"edition": 2025, "nat": "https://x/nat.zip", "dpt": "https://x/dpt.zip", "page": "https://x/p"}
    got = []
    def get(url):
        got.append(url)
        return zipped(NAT if "nat" in url else DPT)
    out = asset.run(tmp_path, None, Log(), find=lambda: found, get=get)
    assert out["skipped"] is False and (tmp_path / "prenoms" / "manifest.json").exists()
    assert json.loads((tmp_path / "prenoms" / "manifest.json").read_text())["edition"] == 2025
    again = asset.run(tmp_path, None, Log(), find=lambda: found, get=get)
    assert again["skipped"] is True and len(got) == 2, "no second download"
    assert not any(p.name.startswith(".prenoms") for p in tmp_path.iterdir()), "no temporary directory left"
