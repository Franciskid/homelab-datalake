"""SSMSI, « Insécurité et délinquance » (annual bilan) and Interstats Info rapide on domestic violence: the formats and the checks.

What each input is, and how it reaches the lake:

* The bilan's **data tables** (a zip of workbooks the SSMSI publishes next to the PDF: « Séries longues 2016-2025 », the per-fiche files, the
  synthèse). The publisher answers 403 to scripts, so the file is a **manual input**, downloaded in a browser and stored with `manual_inputs`
  (sha256, URL, date). It is refreshed by hand each July. Nothing fetches it.
* The bilan **PDF**, which the files host serves to a plain request: the collector fetches it once a year (see assets/ssmsi_bilan.py). It is
  not the source of the figures; it is what the figures are checked against (the synthèse's shares of foreigners among the people charged and
  the population share the SSMSI states).
* Interstats Info rapide on domestic violence (n°56 for 2024, n°69 for 2025): a PDF and, since n°69, a workbook. Manual inputs too.

Nothing here estimates anything. A cell the publisher masks (« secr. », « <5 ») is left out; a check that fails raises, and the collector then
reports the block as failed so that the export keeps yesterday's.
"""

# ruff: noqa: RUF001 -- the typographic quotes and dashes are the publisher's

from __future__ import annotations

import io
import re
import subprocess
import unicodedata
import zipfile

from .xlsx import read as read_xlsx

SOURCE_PAGE = "https://statistiques.interieur.gouv.fr/ssmsi/publications/insecurite-et-delinquance-en-2025-bilan-statistique-et-atlas-departemental"
FILES_HOST = "https://statistiques.interieur.gouv.fr/ssmsi/files/ssmsi"
IR_PAGE = "https://statistiques.interieur.gouv.fr/ssmsi/publications/violences-conjugales-enregistrees-par-les-services-de-securite-nouvelle-hausse-de-5-en"



class PdfLayoutError(ValueError):
    """The PDF does not look the way it did: a reason to say so and to skip the check, never to drop data that the workbooks already prove."""


def norm(text) -> str:
    text = str(text or "").replace("\u2019", "'").replace("\u00a0", " ")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


# ---------------------------------------------------------------- indicators and nationality groups

# (slug, French label, start of the normalised header). The workbooks word the same indicator slightly differently
# (« Vols avec arme » / « Vols avec armes », « paiement » / « paiements »): matched on a stem.
INDICATORS = [
    ("homicides", "Homicides (y compris coups et blessures volontaires suivis de mort)", "homicides"),
    ("tentatives_homicide", "Tentatives d’homicide", "tentatives d homicide"),
    ("violences_hors_famille", "Violences physiques hors cadre familial", "violences physiques hors"),
    ("violences_intrafamiliales", "Violences physiques intrafamiliales", "violences physiques intrafamiliales"),
    ("violences_sexuelles", "Violences sexuelles", "violences sexuelles"),
    ("viols", "Viols et tentatives de viol", "viols et tentatives"),
    ("agressions_sexuelles", "Agressions sexuelles", "agressions sexuelles"),
    ("vols_avec_armes", "Vols avec armes", "vols avec arme"),
    ("vols_violents_sans_arme", "Vols violents sans arme", "vols violents sans arme"),
    ("vols_sans_violence", "Vols sans violence contre des personnes", "vols sans violence"),
    ("cambriolages", "Cambriolages de logement", "cambriolages"),
    ("vols_de_vehicule", "Vols de véhicule", "vols de vehicule"),
    ("vols_dans_vehicules", "Vols dans les véhicules", "vols dans les vehicules"),
    ("vols_accessoires", "Vols d’accessoires sur les véhicules", "vols d accessoires"),
    ("destructions", "Destructions et dégradations volontaires", "destructions et degradations"),
    ("escroqueries", "Escroqueries et fraudes aux moyens de paiement", "escroqueries"),
    ("trafic_stupefiants", "Trafic de stupéfiants", "trafic de stupefiants"),
    ("usage_stupefiants", "Usage de stupéfiants", "usage de stupefiants"),
]
INDICATOR_LABELS = {slug: label for slug, label, _ in INDICATORS}

GROUPS = [
    ("FR", "Français"),
    ("MAGHREB", "Maghreb (Algériens, Marocains, Tunisiens)"),
    ("AFRICA_OTHER", "Autres nationalités d’Afrique"),
    ("ASIA", "Asie"),
    ("AMERICA", "Amérique"),
    ("OCEANIA", "Océanie"),
    ("EU27_SOUTH", "Espagnols, Italiens, Portugais"),
    ("EU27_OTHER", "Autres nationalités de l’Union européenne à 27"),
    ("EUROPE_OTHER", "Autres nationalités d’Europe"),
    ("UNKNOWN", "Nationalité non renseignée"),
    ("TOTAL", "Ensemble"),
]
GROUP_LABELS = dict(GROUPS)
FOREIGN_GROUPS = ["MAGHREB", "AFRICA_OTHER", "ASIA", "AMERICA", "OCEANIA", "EU27_SOUTH", "EU27_OTHER", "EUROPE_OTHER"]
# The workbooks name the same groups differently in the mis en cause and in the victims files.
GROUP_BY_NAME = {
    "france": "FR", "francais": "FR",
    "maghreb": "MAGHREB", "afrique hors maghreb": "AFRICA_OTHER", "asie": "ASIA", "amerique": "AMERICA", "oceanie": "OCEANIA",
    "espagnols italiens portugais": "EU27_SOUTH", "europe du sud": "EU27_SOUTH",
    "autres nationalites d afrique": "AFRICA_OTHER", "autres nationalites d europe": "EUROPE_OTHER",
    "autres nationalites de l union europeenne a 27": "EU27_OTHER", "autre ue27": "EU27_OTHER",
    "europe hors ue27": "EUROPE_OTHER",
    "non renseigne": "UNKNOWN", "non renseignee": "UNKNOWN",
}
DIMS = ["indicator", "group", "time"]


def indicator_slug(header) -> str | None:
    key = norm(header)
    for slug, _label, stem in INDICATORS:
        if key.startswith(stem):
            return slug
    return None


def number(cell):
    """A count, or None for a masked cell (« secr. », « <5 »), an empty one or a text."""
    return float(cell) if isinstance(cell, (int, float)) and not isinstance(cell, bool) else None


def indicator_columns(head, years, *, first: int) -> list[tuple[int, str, float]]:
    """(column, indicator, year) of a series sheet. Only the first column of an indicator (the one of 2016) carries its name: the others carry an
    internal code (« indic_violences_phy_iif »), so a block starts where the year is the first one and goes on until the next 2016."""
    out, slug, seen = [], None, set()
    first_year = min(y for y in years[first:] if isinstance(y, float))
    for c in range(first, len(head)):
        year = years[c] if c < len(years) else None
        if not isinstance(year, float):
            continue
        if year == first_year:
            slug = indicator_slug(head[c])
            if slug is None or slug in seen:
                raise ValueError(f"SSMSI series: column {c} ({head[c]!r}) is not a known indicator")
            seen.add(slug)
        if slug is None:
            raise ValueError("SSMSI series: a year column comes before any indicator")
        out.append((c, slug, year))
    if len(seen) < 15 or len(out) != len(seen) * len({y for _c, _s, y in out}):
        raise ValueError(f"SSMSI series: {len(seen)} indicators, {len(out)} columns: not a rectangle")
    return out


def series_rows(sheet_natio, sheet_majmin) -> list[dict]:
    """Counts by indicator, nationality group and year from the `natio` and `natio_majmin` sheets of Mec_/Victimes_2016_2025.

    The group rows come from `natio`. The total of an indicator is the last two rows of `natio_majmin` (adults and minors, labelled « Total »
    and, by a typo in the file, « Étrangère » on the second line), which this reader takes by position and checks against the groups.
    """
    columns = indicator_columns(sheet_natio[0][1], sheet_natio[1][1], first=1)
    out: dict[tuple, float] = {}
    seen_groups = set()
    for _n, cells in sheet_natio[2:]:
        group = GROUP_BY_NAME.get(norm(cells[0])) if cells and cells[0] else None
        if group is None:
            continue
        seen_groups.add(group)
        for c, slug, year in columns:
            v = number(cells[c]) if c < len(cells) else None
            if v is not None:
                out[(slug, group, str(int(year)))] = v
    if "FR" not in seen_groups or len(seen_groups) < 8:
        raise ValueError(f"SSMSI series: nationality groups missing ({sorted(seen_groups)})")
    # totals: adults + minors, from the two last lines of natio_majmin
    mm_columns = indicator_columns(sheet_majmin[0][1], sheet_majmin[1][1], first=2)
    lines = [cells for _n, cells in sheet_majmin[2:] if cells]
    if len(lines) < 6 or norm(lines[-2][0]) != "total":
        raise ValueError("SSMSI series: the total lines of natio_majmin are not where they were")
    adults, minors = lines[-2], lines[-1]
    for c, slug, year in mm_columns:
        a, m = number(adults[c]), number(minors[c])
        if a is not None and m is not None:
            out[(slug, "TOTAL", str(int(year)))] = a + m
    return [{"indicator": k[0], "group": k[1], "time": k[2], "value": v} for k, v in sorted(out.items())]


def check_series(rows: list[dict], *, kind: str) -> dict:
    """Every indicator and year has its total and the French count, and the groups never add up to more than the total.

    The groups rarely add up to it: the publisher masks small cells (« <5 ») and, to stop them being recomputed, some larger ones (« secr. ») in the
    same table, so the difference is the masked cells and is not bounded here. What is exact is the total less the French count: everyone who is not
    recorded as French (a foreign nationality, or none noted).
    """
    by = {}
    for r in rows:
        by.setdefault((r["indicator"], r["time"]), {})[r["group"]] = r["value"]
    years = sorted({r["time"] for r in rows})
    if years[0] != "2016" or years[-1] < "2025" or len(years) != int(years[-1]) - 2015:
        raise ValueError(f"SSMSI {kind}: years {years[0]}..{years[-1]}")
    checked, masked = 0, 0.0
    for (slug, year), g in by.items():
        if "TOTAL" not in g or "FR" not in g:
            raise ValueError(f"SSMSI {kind}: {slug} {year} has no total or no French count")
        gap = g["TOTAL"] - sum(v for k, v in g.items() if k != "TOTAL")
        if gap < -0.5:
            raise ValueError(f"SSMSI {kind}: {slug} {year}: the groups ({sum(v for k, v in g.items() if k != 'TOTAL')}) exceed the total ({g['TOTAL']})")
        masked = max(masked, gap / g["TOTAL"])
        checked += 1
    return {"years": f"{years[0]}-{years[-1]}", "totals_checked": checked, "max_masked_share": round(masked, 4)}


# ---------------------------------------------------------------- the synthèse, to check the series against

def _label_slug(label) -> str | None:
    return indicator_slug(re.sub(r"^\s*(dont|>)\s*", "", str(label or "").strip(), flags=re.I))


def synthese_profile(sheets: dict) -> dict:
    """Figure 7 (profile of the people charged in 2025): share of foreigners per indicator, as the SSMSI prints it (column « Etrangers »)."""
    out: dict[str, float] = {}
    for _n, cells in sheets["Figure 7"]:
        if cells and len(cells) > 11 and isinstance(cells[11], float):
            slug = _label_slug(cells[1])
            if slug:
                out[slug] = cells[11]
    return out


def synthese_changes(sheets: dict) -> dict:
    """Figure 8: share of foreigners among victims and among the people charged in 2025 and its change since 2016 in points: {slug: {...}}."""
    out: dict[str, dict] = {}
    for _n, cells in sheets["Figure 8"]:
        if not cells or len(cells) < 9:
            continue
        slug = _label_slug(cells[0])
        if slug and all(isinstance(cells[i], float) or cells[i] is None for i in range(5, 9)) and cells[7] is not None:
            out[slug] = {"victims": cells[5], "victims_change": cells[6], "charged": cells[7], "charged_change": cells[8]}
    return out


def share_foreign(by_group: dict) -> float:
    """Percentage of people recorded as other than French (the SSMSI's own definition: a nationality not noted counts with the foreign ones)."""
    return 100 * (by_group["TOTAL"] - by_group["FR"]) / by_group["TOTAL"]


def check_against_synthese(mec: list[dict], victims: list[dict], profile: dict, changes: dict, *, tolerance: float = 0.05) -> dict:
    """The shares of foreigners and their 2016 → 2025 changes recomputed from the series against the synthèse's Figures 7 and 8 (percentage points).

    The SSMSI prints them to three decimals, so the series must give the same: a gap beyond 0.05 point means the two files are not from the same extraction.
    """
    def table(rows):
        out: dict[tuple, dict] = {}
        for r in rows:
            out.setdefault((r["indicator"], r["time"]), {})[r["group"]] = r["value"]
        return out

    m, v = table(mec), table(victims)
    last = max(r["time"] for r in mec)
    compared, worst = 0, 0.0

    def same(label, mine, printed):
        nonlocal compared, worst
        gap = abs(mine - printed)
        worst = max(worst, gap)
        if gap > tolerance:
            raise ValueError(f"SSMSI: {label}: the series give {mine:.3f}, the synthèse prints {printed:.3f}")
        compared += 1

    for slug, printed in profile.items():
        if (slug, last) in m:
            same(f"{slug} charged {last}", share_foreign(m[(slug, last)]), printed)
    for slug, c in changes.items():
        if (slug, last) in m and (slug, "2016") in m and "TOTAL" in m[(slug, "2016")] and "FR" in m[(slug, "2016")]:
            same(f"{slug} charged change", share_foreign(m[(slug, last)]) - share_foreign(m[(slug, "2016")]), c["charged_change"])
        if c["victims"] is not None and (slug, last) in v and (slug, "2016") in v:
            same(f"{slug} victims {last}", share_foreign(v[(slug, last)]), c["victims"])
            same(f"{slug} victims change", share_foreign(v[(slug, last)]) - share_foreign(v[(slug, "2016")]), c["victims_change"])
    if compared < 40:
        raise ValueError(f"SSMSI: only {compared} figures could be compared with the synthèse")
    return {"compared_with_synthese": compared, "max_gap_points": round(worst, 4)}


# ---------------------------------------------------------------- the PDF (pdftotext: PDFs are untrusted input, opened through it only)

def pdf_text(data: bytes) -> list[str]:
    """Text of every page, in reading order (poppler's pdftotext; a form feed ends a page)."""
    done = subprocess.run(["pdftotext", "-layout", "-", "-"], input=data, capture_output=True, check=True, timeout=180)  # noqa: S607
    return done.stdout.decode("utf-8", "replace").split("\f")


def pdf_profile(pages: list[str]) -> dict:
    """From the synthèse's Figure 7 in the PDF: the « Étrangers » column (the last figure of each line) and the population share the SSMSI states
    (« Part dans la population en France »). An indicator's name may run over two lines: the figures sit on the second.

    Returns {"page": n, "population_pct": [...], "foreign": {slug: %}} read from the printed text only.
    """
    for number_, text in enumerate(pages, 1):
        if "Profil des mis en cause" in text and "Part dans la population en France" in text and "Étrangers" in text:
            lines = text.split("\n")
            pop = next(line for line in lines if "Part dans la population en France" in line)
            pct = [float(x.replace(",", ".")) for x in re.findall(r"(\d+(?:,\d+)?)\s*%", pop)]
            foreign: dict[str, float] = {}
            previous = ""
            for line in lines:
                m = re.match(r"^\s*([^\d]+?)\s{2,}(\d.*)$", line)
                if not m:
                    previous = line.strip() or previous
                    continue
                figures = re.findall(r"\d+", m.group(2))
                label = m.group(1).strip()
                # A name that runs over two lines: its second line starts in lower case (« hors cadre familial », « contre des personnes »), and the
                # first line may already carry figures (the age bands), so the foreigners' figure is the last one of the second line.
                own = _label_slug(label)
                slug = own or (_label_slug(f"{previous} {label}") if label[:1].islower() else None)
                if slug and figures and (slug not in foreign or own is None):
                    foreign[slug] = float(figures[-1])
                previous = label
            return {"page": number_, "population_pct": pct, "foreign": foreign}
    raise PdfLayoutError("the figure of the profile of the people charged is not where it was")


def check_against_pdf(profile: dict, printed: dict, *, tolerance: float = 0.55) -> dict:
    """The synthèse workbook's shares against the integers the PDF prints (rounded to the unit: 0.5 point apart at most)."""
    compared, worst = 0, 0.0
    for slug, value in printed["foreign"].items():
        if slug in profile:
            worst = max(worst, abs(profile[slug] - value))
            if abs(profile[slug] - value) > tolerance:
                raise ValueError(f"SSMSI: {slug}: the workbook says {profile[slug]:.2f} %, the PDF (page {printed['page']}) prints {value:.0f} %")
            compared += 1
    if compared < 8:
        raise PdfLayoutError(f"only {compared} indicators could be compared")
    return {"compared_with_pdf": compared, "pdf_page": printed["page"], "max_gap_points_pdf": round(worst, 3)}


def population_stated(printed: dict) -> float:
    """The foreigners' share of the population the bilan states: the last figure of the line « Part dans la population en France » (« 9 % »)."""
    if not printed["population_pct"]:
        raise PdfLayoutError("no population share is printed")
    return printed["population_pct"][-1]


def pdf_edition(pages: list[str]) -> int | None:
    m = re.search(r"[ée]dition\s+(20\d\d)", " ".join(pages[:3]), re.I)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------- population shares of the nationality groups (fiche tables)

def group_population_shares(sheet) -> dict[str, float]:
    """« Maghreb (Algériens, Marocains, Tunisiens) [2 %] » : the share of the population the SSMSI prints in each group's label, in whole points.

    « <1 % » is not a number and is left out.
    """
    out: dict[str, float] = {}
    for _n, cells in sheet:
        if not cells or not isinstance(cells[0], str):
            continue
        label = cells[0].replace("\xa0", " ")
        m = re.search(r"[\(\[]\s*(\d+(?:[.,]\d+)?)\s*%\s*[\)\]]", label)
        if not m:
            continue
        head = re.sub(r"\s*[\(\[].*$", "", label).strip().rstrip(":").strip()
        group = GROUP_BY_NAME.get(norm(head))
        if group is None and norm(head).startswith("etrangers"):
            group = "FOREIGN"
        if group:
            out[group] = float(m.group(1).replace(",", "."))
    return out


# ---------------------------------------------------------------- reading the zip

def zip_members(data: bytes) -> dict[str, bytes]:
    """Every workbook of the zip by file name, without the `~$` lock files (Excel leaves them next to an open file)."""
    out = {}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for info in z.infolist():
            name = info.filename
            if info.is_dir() or name.rsplit("/", 1)[-1].startswith("~$") or not name.lower().endswith(".xlsx"):
                continue
            if info.file_size > 60_000_000:
                raise ValueError(f"SSMSI zip: {name} is too large")
            out[name] = z.read(name)
    return out


def find(members: dict[str, bytes], *needles: str, exclude: tuple[str, ...] = ()) -> bytes:
    hits = [n for n in members if all(norm(x) in norm(n) for x in needles) and not any(norm(x) in norm(n) for x in exclude)]
    if len(hits) != 1:
        raise ValueError(f"SSMSI zip: {len(hits)} files match {needles}")
    return members[hits[0]]


def bilan_tables(zip_bytes: bytes) -> dict:
    """Everything the pages use from the data zip, checked against its own synthèse: mis en cause and victims counts by nationality group,
    the nationality groups' population shares printed by the SSMSI, and the checks run."""
    members = zip_members(zip_bytes)
    mec = read_xlsx(find(members, "mec 2016"))
    victims = read_xlsx(find(members, "victimes 2016"))
    rows_mec = series_rows(mec["natio"], mec["natio_majmin"])
    rows_victims = series_rows(victims["natio"], victims["natio_majmin"])
    checks = {"mec": check_series(rows_mec, kind="mec"), "victims": check_series(rows_victims, kind="victims")}
    synth = read_xlsx(find(members, "synthese", "edition", exclude=("tableau de synthese",)))
    synth = {k.strip(): v for k, v in synth.items()}
    profile = synthese_profile(synth)
    checks["synthese"] = check_against_synthese(rows_mec, rows_victims, profile, synthese_changes(synth))
    pops = group_population_shares(read_xlsx(find(members, "homicides", exclude=("tentatives",)))["Figure 10"])
    return {"mec": rows_mec, "victims": rows_victims, "profile": profile, "population_groups": pops, "checks": checks}


# ---------------------------------------------------------------- Interstats Info rapide: violences conjugales

def ir_categories():
    return [("total", "Total", "total"), ("homicide", "Homicide", "homicide"), ("tentative", "Tentative d’homicide", "tentative d homicide"),
            ("physiques", "Violences physiques et administration de substances nuisibles", "violences physiques et administration"),
            ("sexuelles", "Violences sexuelles", "violences sexuelles"),
            ("psychologiques", "Violences verbales et psychologiques", "violences verbales")]


def ir69_rows(data: bytes, year: int = 2025) -> list[dict]:
    """Info rapide n°69 (violences conjugales 2025): people recorded as victims (Figure 1) and charged (Figure 6), with the share of French people.

    Rows {side, category, time, measure, value}: measures « n » (count), « french_pct » (share of French people printed, whole points), and the previous year's count.
    """
    sheets = read_xlsx(data)
    out: list[dict] = []
    for side, sheet in (("victims", "Figure 1"), ("charged", "Figure 6")):
        found = {}
        for _n, cells in sheets[sheet]:
            if not cells or not cells[0]:
                continue
            key = norm(cells[0])
            for slug, _label, stem in ir_categories():
                if slug not in found and key.startswith(stem) and isinstance(cells[1], float):
                    found[slug] = cells
        if "total" not in found or "physiques" not in found:
            raise ValueError(f"IR69: {sheet} has no total line")
        french_col = 4
        for slug, cells in found.items():
            if isinstance(cells[french_col], float):
                out.append({"side": side, "category": slug, "time": str(year), "measure": "french_pct", "value": cells[french_col]})
            out.append({"side": side, "category": slug, "time": str(year), "measure": "n", "value": cells[1]})
            if isinstance(cells[5], float):
                out.append({"side": side, "category": slug, "time": str(year - 1), "measure": "n_revised", "value": cells[5]})
    return out


def ir56_rows(pages: list[str], year: int = 2024) -> list[dict]:
    """Info rapide n°56 (violences conjugales 2024), from the PDF's two tables (Figure 1 victims, Figure 6 people charged).

    A category's name may run over several lines with the figures on one of them: the figures of a line belong to the name on that line
    (« Total », « Homicide », « Violences sexuelles ») or, for the two long names, to the text just above (« Violences physiques / et administration
    de ... », « Violences verbales / ... »).
    """
    wanted = {"total": ("own", "total"), "homicide": ("own", "homicide"), "tentative": ("own", "tentative d homicide"),
              "physiques": ("window", "violences physiques et administration"), "sexuelles": ("own", "violences sexuelles"),
              "psychologiques": ("window", "violences verbales")}
    out: list[dict] = []
    for side, marker in (("victims", "Figure 1 – Victimes de violences conjugales"), ("charged", "Figure 6 – Mis en cause")):
        page = next((p for p in pages if marker in p), None)
        if page is None:
            raise ValueError(f"IR56: {marker} is not in the PDF")
        found: dict[str, list[float]] = {}
        texts: list[str] = []
        for line in page.split("\n"):
            stripped = line.strip()
            # figures: thousands are separated by a single space ("272 382"), columns by wider gaps
            tokens = [t for t in re.split(r"\s{2,}", stripped.replace("\u00a0", " ")) if t]
            head = [t for t in tokens if re.search(r"[A-Za-zÀ-ÿ]", t)]
            figures = [t.replace(" ", "") for t in tokens if not re.search(r"[A-Za-zÀ-ÿ]", t)]
            if len(figures) < 5 or len(head) > 1 or not all(re.fullmatch(r"-?\d+|<1", f) for f in figures):
                if stripped:
                    texts.append(stripped)
                continue
            own = head[0] if head else ""
            nums = [0.5 if f == "<1" else float(f) for f in figures]
            window = norm(" ".join([*texts[-2:], own]))
            for slug, (mode, stem) in wanted.items():
                key = norm(own) if mode == "own" else window
                if slug not in found and key.startswith(stem) and len(nums) >= 5:
                    found[slug] = nums
            texts = []
        if "total" not in found or "physiques" not in found:
            raise ValueError(f"IR56: the total or the physical-violence line of {marker} was not read")
        for slug, nums in found.items():
            out.append({"side": side, "category": slug, "time": str(year), "measure": "n", "value": nums[0]})
            out.append({"side": side, "category": slug, "time": str(year), "measure": "french_pct", "value": nums[3]})
    return out
