# Vendored from the politico repository (scripts/pollslib/discover.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Where new reports show up: the Commission des sondages list, the institutes' pages, the French Wikipedia table.

Everything here parses text it is given (no network, no files): the caller fetches, so the same code runs on fixtures in
tests and on live pages in the daily job. A page that changes layout yields no candidates and is reported by the caller as a
source that "found nothing"; it never raises into the job.
"""
import html as _html
import re
from datetime import date
from urllib.parse import urljoin, urlsplit

MONTHS = {"janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6, "juillet": 7, "aout": 8, "septembre": 9,
          "octobre": 10, "novembre": 11, "decembre": 12}
INSTITUTES = [("ifop", "Ifop"), ("elabe", "Elabe"), ("ipsos", "Ipsos"), ("opinionway", "OpinionWay"), ("harris", "Harris"), ("toluna", "Harris"),
              ("odoxa", "Odoxa"), ("cluster17", "Cluster17"), ("verian", "Verian"), ("kantar", "Verian"), ("yougov", "YouGov"), ("bva", "BVA"), ("csa", "CSA")]


def _norm(s: str) -> str:
    import unicodedata
    s = _html.unescape(s).lower().replace("œ", "oe")
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def institute_of(text: str) -> str | None:
    n = re.sub(r"[^a-z0-9]", "", _norm(text))
    for key, name in INSTITUTES:
        if key in n:
            return name
    return None


def text_of(fragment: str) -> str:
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", fragment))).strip()


# ---- Commission des sondages -----------------------------------------------------------------

def parse_commission_notices(page: str, base: str = "https://www.commission-des-sondages.fr/notices/") -> list[dict]:
    """[{no, title, institute, published, url, vote_intentions}] — `published` is the date in the title, in the month's heading."""
    out = []
    for block in re.split(r'<h2 class="notices-mois-titre"[^>]*>', page)[1:]:
        head, _, rest = block.partition("</h2>")
        m = re.match(r"\s*([^\W\d_]+)\s+(20\d\d)", _html.unescape(head))
        if not m or _norm(m.group(1)) not in MONTHS:
            continue
        year = int(m.group(2))
        for a in re.finditer(r'<a href="([^"]+)"[^>]*class="pdf_download"[^>]*>(.*?)</a>', rest, re.S):
            title = text_of(a.group(2))
            t = re.match(r"(\d{4,6})\s+(.*?)\s+(\d{1,2})(?:er)?\s+([^\W\d_]+)\s*$", title)
            if not t or _norm(t.group(4)) not in MONTHS:
                continue
            try:
                published = date(year, MONTHS[_norm(t.group(4))], int(t.group(3)))
            except ValueError:
                continue
            label = t.group(2)
            out.append({"no": int(t.group(1)), "title": title, "institute": institute_of(label), "published": published.isoformat(),
                        "url": urljoin(base, a.group(1)),
                        "vote_intentions": bool(re.match(r"(?i)pres\b", label) and re.search(r"(?i)\bIV\b", label))})
    return out


# ---- institutes' pages -----------------------------------------------------------------------

REPORT_URL = re.compile(r"presidentielle|presitrack|2027|intentions|intenzioni|election|barometre|sondage|enquete-electorale|cevipof|/medias/fichiers/|/fichier-pdf/", re.I)
REPORT_WORDS = re.compile(r"intentions?[- ]de[- ]vote|presidentielle|présidentielle|presitrack|barometre|baromètre|sondage|election", re.I)


def links(page: str, base: str) -> list[tuple[str, str]]:
    out, seen = [], set()
    for m in re.finditer(r'<a\b[^>]*?href="([^"#]+)"[^>]*>(.*?)</a>', page, re.S):
        u = urljoin(base, _html.unescape(m.group(1)))
        if urlsplit(u).scheme in ("http", "https") and u not in seen:
            seen.add(u)
            out.append((u, text_of(m.group(2))))
    return out


def pdf_links(page: str, base: str) -> list[str]:
    """Links to PDFs on a page (extension, or Elabe's /fichier-pdf/ and the Commission's /fichiers/ downloads)."""
    return [u for u, _ in links(page, base) if re.search(r"\.pdf(?:$|\?)|/fichier-pdf/|/medias/fichiers/", u, re.I)]


def article_links(page: str, base: str, pattern: str) -> list[str]:
    rx = re.compile(pattern, re.I)
    return [u for u, t in links(page, base) if rx.search(u) or rx.search(t)]


# Listing pages and the links worth following. Each is read alone: one that fails or changes layout is reported, not fatal.
SOURCES = [
    {"institute": "Elabe", "listing": ["https://elabe.fr/tag/presidentielle-2027/"], "article": r"elabe\.fr/presidentielle-2027"},
    {"institute": "Ifop", "listing": ["https://www.ifopgroup.com/?s=pr%C3%A9sidentielle+2027"], "article": r"ifopgroup\.com/article/.*(intentions-de-vote|presidentielle-2027)"},
    {"institute": "OpinionWay", "listing": ["https://www.opinion-way.com/fr/publications/?publication_format_terms=69"], "article": r"publications/(presitrack|.*presidentielle|.*intentions)"},
    {"institute": "Harris", "listing": ["https://tolunacorporate.com/fr/"], "article": r"tolunacorporate\.com/fr/(intentions-de-vote|.*presidentielle)"},
    {"institute": "Cluster17", "listing": ["https://cluster17.com/nos-sondages/"], "article": r"cluster17\.com/(.*presidentielle|.*intentions|wp-content/uploads/.*\.pdf)"},
    {"institute": "Ipsos", "listing": ["https://www.ipsos.com/fr-fr/search?keys=intentions%20de%20vote%20pr%C3%A9sidentielle%202027"], "article": r"ipsos\.com/fr-fr/.*(presidentielle|intentions)"},
    {"institute": "Odoxa", "listing": ["https://www.odoxa.fr/"], "article": r"odoxa\.fr/.*(presidentielle|sondage)"},
    {"institute": "Verian", "listing": ["https://www.veriangroup.com/"], "article": r"(presidentielle|barometre)"},
    {"institute": "YouGov", "listing": ["https://yougov.com/fr-fr/"], "article": r"yougov\.com/fr-fr/.*(presidentielle|intentions)"},
    {"institute": "BVA", "listing": ["https://www.bva-xsight.com/"], "article": r"(presidentielle|intentions-de-vote)"},
]


# ---- French Wikipedia ------------------------------------------------------------------------

WIKI_URL = "https://fr.wikipedia.org/wiki/Liste_de_sondages_sur_l%27%C3%A9lection_pr%C3%A9sidentielle_fran%C3%A7aise_de_2027"
_DATE = re.compile(r"(\d{1,2})(?:er)?(?:\s*(?:-|–|au)\s*|\s+)?(?:(\d{1,2})(?:er)?\s+)?([^\W\d_]+)?\s*(?:(\d{1,2})\s+([^\W\d_]+)\s+)?(20\d\d)")


def _wiki_dates(cell: str):
    """'12-13 novembre 2026' / '30 octobre - 2 novembre 2026' / '3 novembre 2026' -> (start, end) or None."""
    c = _norm(cell).replace("–", "-")
    m = re.match(r"^(\d{1,2})(?:er)? ?(?:([a-z]+))? ?(?:-|au) ?(\d{1,2})(?:er)? ([a-z]+) (20\d\d)$", c)
    if m:
        d1, m1, d2, m2, y = m.groups()
        mm2 = MONTHS.get(m2)
        mm1 = MONTHS.get(m1) if m1 else mm2
        if mm1 and mm2:
            y = int(y)
            return date(y - (1 if mm1 > mm2 else 0), mm1, int(d1)), date(y, mm2, int(d2))
    m = re.match(r"^(\d{1,2})(?:er)? ([a-z]+) (20\d\d)$", c)
    if m and MONTHS.get(m.group(2)):
        d = date(int(m.group(3)), MONTHS[m.group(2)], int(m.group(1)))
        return d, d
    return None


def parse_wikipedia_polls(page: str) -> list[dict]:
    """First-round and runoff rows of the French Wikipedia list: [{institute, start, end, sample}] (one entry per poll)."""
    seen, out = set(), []
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", page, re.S):
        cells = [text_of(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if len(cells) < 3:
            continue
        inst = None
        for c in cells[:2]:
            inst = institute_of(c.rsplit(">", 1)[-1]) or inst
        dates = next((_wiki_dates(c) for c in cells[:4] if _wiki_dates(c)), None)
        if not inst or not dates:
            continue
        sample = next((int(re.sub(r"\D", "", c)) for c in cells[1:5] if re.fullmatch(r"\d{1,3}(?:[  ]\d{3})*|\d{3,6}", c) and 300 <= int(re.sub(r"\D", "", c)) <= 50000), None)
        key = (inst, dates[1].isoformat())
        if key in seen:
            continue
        seen.add(key)
        out.append({"institute": inst, "start": dates[0].isoformat(), "end": dates[1].isoformat(), "sample": sample})
    return out
