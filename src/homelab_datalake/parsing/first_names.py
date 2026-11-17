"""The Insee "Fichier des prénoms" (births in France since 1900, Licence Ouverte), cut into small files a static site can load on demand.

The file is published once a year (July) as zips of CSV, on a page whose number changes with each edition. The series page
(SERIES_PAGE) links to it, so a new edition is found without a code change. Two files are used:

* `nat`: `sexe;prenom;periode;valeur;rang`, every first name by sex and year of birth, France (counts rounded to the nearest multiple of 5);
* `dpt`: the same by département of birth (`dpt`), names given often enough to be published in the département.

Names given too rarely are not in the files at all (nor as a grouped "rare names" line): the totals of a year are the totals of the
published names, not the births of the year. The chunks keep that visible: `births` holds those totals and nothing here pretends
otherwise.

What is written (parts of the site's `/donnees/data/prenoms-<edition>-<hash>/`):

    manifest.json   edition, years, decades, départements, letters and their files, the published births per year and sex, the 50 most given names
    tops.json       the 20 most given names of every year and sex
    dep-births.json the published births per sex, département and decade (the denominator of a map)
    nat-<L>.json    for each name and sex of the letter, its count per year (counts/5, from the first year it appears)
    dep-<L>.json    for each name and sex given at least MIN_DEP times in the départements, its counts/5 per decade and département

Pure functions; assets/first_names.py fetches and writes.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
import zipfile
from collections import defaultdict

SERIES_PAGE = "https://www.insee.fr/fr/statistiques/8595113"
INSEE = "https://www.insee.fr"
FIRST_YEAR = 1900
MIN_DEP = 200  # a name needs this many births in the départements' file to get a map (below, a map is noise)
TOP = 20
SEXES = ("1", "2")  # 1 boys, 2 girls (the Insee's codes)


def letter(name: str) -> str:
    """The file a name is in: its first letter without accent, `_` when it is not a letter."""
    c = unicodedata.normalize("NFD", name[:1].upper())[:1]
    return c if "A" <= c <= "Z" else "_"


def decade_index(year: int) -> int:
    return (year - FIRST_YEAR) // 10


# ---------------------------------------------------------------- finding the edition

def edition_links(html: str) -> dict | None:
    """From a page of the Fichier des prénoms: its edition and the two CSV zips, or None if it is another page."""
    nat = re.search(r'href="(/fr/statistiques/fichier/\d+/prenoms-(\d{4})-nat_csv\.zip)"', html)
    dpt = re.search(r'href="(/fr/statistiques/fichier/\d+/prenoms-(\d{4})-dpt_csv\.zip)"', html)
    if not nat or not dpt or nat.group(2) != dpt.group(2):
        return None
    return {"edition": int(nat.group(2)), "nat": INSEE + nat.group(1), "dpt": INSEE + dpt.group(1)}


def candidate_pages(series_html: str) -> list[str]:
    ids = re.findall(r'href="/fr/statistiques/(\d+)\?sommaire=8595113"', series_html)
    return [f"{INSEE}/fr/statistiques/{i}" for i in dict.fromkeys(ids)]


# ---------------------------------------------------------------- reading

def csv_in_zip(content: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(f"expected one CSV in the zip, found {names[:5]}")
        return z.read(names[0]).decode("utf-8-sig")


def read_nat(text: str) -> list[tuple[str, str, int, int]]:
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    if {"sexe", "prenom", "periode", "valeur"} - set(reader.fieldnames or ()):
        raise ValueError("fichier des prénoms (national): columns are gone")
    out = []
    for r in reader:
        if r["sexe"] not in SEXES or not r["periode"].isdigit() or not r["valeur"].isdigit():
            continue
        out.append((r["prenom"].strip(), r["sexe"], int(r["periode"]), int(r["valeur"])))
    return out


def read_dpt(text: str) -> list[tuple[str, str, int, str, int]]:
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    if {"sexe", "prenom", "periode", "dpt", "valeur"} - set(reader.fieldnames or ()):
        raise ValueError("fichier des prénoms (départements): columns are gone")
    out = []
    for r in reader:
        if r["sexe"] not in SEXES or not r["periode"].isdigit() or not r["valeur"].isdigit():
            continue
        out.append((r["prenom"].strip(), r["sexe"], int(r["periode"]), r["dpt"].strip(), int(r["valeur"])))
    return out


# ---------------------------------------------------------------- cutting

def build(nat: list, dpt: list, edition: int, source: dict) -> dict[str, dict]:
    """File name -> JSON-able content. `manifest.json` is last in the dict so that a writer that writes in order ends with it."""
    years = sorted({y for _, _, y, _ in nat})
    if not years or years[0] > FIRST_YEAR + 5 or years[-1] < edition - 1:
        raise ValueError(f"the national file covers {years[:1]}..{years[-1:]} for edition {edition}")
    y0, y1 = years[0], years[-1]
    ny = y1 - y0 + 1

    series: dict[tuple[str, str], list[int]] = {}
    totals = {s: [0] * ny for s in SEXES}
    for name, sex, year, n in nat:
        if name.startswith("_"):  # a grouped line of rare names counts in the totals only
            totals[sex][year - y0] += n
            continue
        arr = series.setdefault((name, sex), [0] * ny)
        arr[year - y0] += n
        totals[sex][year - y0] += n

    deps = sorted({d for _, _, _, d, _ in dpt})
    didx = {d: i for i, d in enumerate(deps)}
    nd = decade_index(y1) + 1
    dep_births = {s: [[0] * nd for _ in deps] for s in SEXES}
    cells: dict[tuple[str, str], dict[tuple[int, int], int]] = defaultdict(dict)
    sums: dict[tuple[str, str], int] = defaultdict(int)
    for name, sex, year, dep, n in dpt:
        dec = decade_index(year)
        dep_births[sex][didx[dep]][dec] += n
        if name.startswith("_"):
            continue
        key = (name, sex)
        cells[key][(dec, didx[dep])] = cells[key].get((dec, didx[dep]), 0) + n
        sums[key] += n

    letters: dict[str, dict] = {}
    files: dict[str, dict] = {}
    by_letter: dict[str, list] = defaultdict(list)
    for (name, sex), arr in series.items():
        first = next(i for i, v in enumerate(arr) if v)
        last = max(i for i, v in enumerate(arr) if v)
        by_letter[letter(name)].append((sum(arr), name, sex, first, arr[first:last + 1]))
    for L, items in sorted(by_letter.items()):
        items.sort(key=lambda t: (-t[0], t[1], t[2]))
        files[f"nat-{L}.json"] = {"letter": L, "y0": y0, "names": [[name, int(sex), y0 + first, [v // 5 for v in arr]] for _, name, sex, first, arr in items]}
        letters[L] = {"nat": f"nat-{L}.json", "names": len(items)}
    dep_letter: dict[str, list] = defaultdict(list)
    for (name, sex), c in cells.items():
        if sums[(name, sex)] >= MIN_DEP:
            flat = [x for (dec, di), n in sorted(c.items()) for x in (dec, di, n // 5)]
            dep_letter[letter(name)].append((sums[(name, sex)], name, sex, flat))
    for L, items in sorted(dep_letter.items()):
        items.sort(key=lambda t: (-t[0], t[1], t[2]))
        files[f"dep-{L}.json"] = {"letter": L, "names": [[name, int(sex), flat] for _, name, sex, flat in items]}
        letters.setdefault(L, {"names": 0})["dep"] = f"dep-{L}.json"

    tops: dict[str, dict] = {}
    for yi, year in enumerate(range(y0, y1 + 1)):
        tops[str(year)] = {}
    by_year: dict[tuple[int, str], list] = defaultdict(list)
    for name, sex, year, n in nat:
        if not name.startswith("_"):
            by_year[(year, sex)].append((n, name))
    for (year, sex), lst in by_year.items():
        lst.sort(key=lambda t: (-t[0], t[1]))
        tops[str(year)][sex] = [[name, n] for n, name in lst[:TOP]]

    overall = sorted(((sum(arr), name, sex) for (name, sex), arr in series.items()), reverse=True)[:50]
    manifest = {
        "edition": edition, "source": source, "years": [y0, y1], "decades": [FIRST_YEAR + 10 * i for i in range(nd)], "deps": deps,
        "letters": letters, "min_dep": MIN_DEP, "rounding": 5, "births": {s: totals[s] for s in SEXES},
        "overall": [[name, int(sex), total] for total, name, sex in overall], "names": {"nat": len(series), "dep": sum(len(v) for v in dep_letter.values())},
        "files": {"tops": "tops.json", "dep_births": "dep-births.json"},
    }
    files["tops.json"] = tops
    files["dep-births.json"] = dep_births
    files["manifest.json"] = manifest
    return files
