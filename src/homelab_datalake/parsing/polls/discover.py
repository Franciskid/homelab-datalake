# Vendored from the politico repository (scripts/pollslib/discover.py, 6db2c47). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Where new reports show up: the Commission des sondages list, the institutes' pages, the French Wikipedia table.

Everything here parses text it is given (no network, no files): the caller fetches, so the same code runs on fixtures in
tests and on live pages in the daily job. A page that changes layout yields no candidates and is reported by the caller as a
source that "found nothing"; it never raises into the job.
"""
import html as _html
import re
from datetime import date
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote, unquote, urljoin, urlsplit

MONTHS = {"janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6, "juillet": 7, "aout": 8, "septembre": 9,
          "octobre": 10, "novembre": 11, "decembre": 12}
INSTITUTES = [("ifop", "Ifop"), ("elabe", "Elabe"), ("ipsos", "Ipsos"), ("opinionway", "OpinionWay"), ("harris", "Harris"), ("toluna", "Harris"),
              ("odoxa", "Odoxa"), ("cluster17", "Cluster17"), ("verian", "Verian"), ("kantar", "Verian"), ("yougov", "YouGov"), ("bva", "BVA"), ("csa", "CSA"), ("viavoice", "Viavoice")]


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

COMMISSION_URL = "https://www.commission-des-sondages.fr/notices/"


def parse_commission_notices(page: str, base: str = COMMISSION_URL) -> list[dict]:
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
                        "vote_intentions": bool(re.match(r"pres(?:identielle)?\b", _norm(label)) and
                                                re.search(r"\b(?:iv|presitrack|intentions? de vote)\b", _norm(label)))})
    return out


# ---- institutes' pages -----------------------------------------------------------------------

REPORT_URL = re.compile(r"presidentielle|presitrack|2027|intentions|intenzioni|election|barometre|sondage|enquete-electorale|cevipof|/medias/fichiers/|/fichier-pdf/", re.I)
REPORT_WORDS = re.compile(r"intentions?[- ]de[- ]vote|presidentielle|présidentielle|presitrack|barometre|baromètre|sondage|election", re.I)


class PageLinks(HTMLParser):
    def __init__(self, base):
        super().__init__(convert_charrefs=True)
        self.base = base
        self.href = None
        self.label = []
        self.links = {}

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.href = dict(attrs).get("href")
            self.label = []

    def handle_data(self, data):
        if self.href:
            self.label.append(data)

    def handle_endtag(self, tag):
        if tag != "a" or not self.href:
            return
        if not self.href.startswith("#"):
            url = quote(urljoin(self.base, self.href), safe=":/?#[]@!$&'()*+,;=%")
            if urlsplit(url).scheme == "https":
                label = " ".join("".join(self.label).split())
                if not self.links.get(url):
                    self.links[url] = label
        self.href = None


def links(page: str, base: str) -> list[tuple[str, str]]:
    parser = PageLinks(base)
    parser.feed(page)
    return list(parser.links.items())


def pdf_links(page: str, base: str) -> list[str]:
    """Include embedded PDF-viewer download URLs (used by Odoxa)."""
    urls = []
    for url, _ in links(page, base):
        parsed = urlsplit(url)
        for target in [url, *parse_qs(parsed.query).get("file", [])]:
            if urlsplit(target).scheme == "https" and re.search(r"\.pdf$|/fichier-pdf/|/medias/fichiers/", urlsplit(target).path, re.I):
                urls.append(target)
    return list(dict.fromkeys(urls))


def article_links(page: str, base: str, pattern: str) -> list[str]:
    rx = re.compile(pattern, re.I)
    return [url for url, label in links(page, base)
            if urlsplit(url).path != urlsplit(base).path and urlsplit(url).netloc.removeprefix("www.") == urlsplit(base).netloc.removeprefix("www.")
            and rx.search(_norm(unquote(urlsplit(url).path) + " " + label))]


# Listing pages and the links worth following. Each is read alone: one that fails or changes layout is reported, not fatal.
SOURCES = [
    {"institute": "Elabe", "listing": ["https://elabe.fr/tag/presidentielle-2027/"], "article": r"/presidentielle-2027"},
    {"institute": "Ifop", "listing": ["https://www.ifopgroup.com/?s=pr%C3%A9sidentielle+2027"], "article": r"/article/.*(intentions-de-vote|presidentielle-2027)"},
    {"institute": "OpinionWay", "listing": ["https://www.opinion-way.com/fr/publications/?publication_format_terms=69"], "article": r"publications/(presitrack|.*presidentielle|.*intentions)"},
    {"institute": "Harris", "listing": ["https://tolunacorporate.com/fr/"], "article": r"/fr/(intentions-de-vote|.*presidentielle)"},
    {"institute": "Cluster17", "listing": ["https://cluster17.com/nos-sondages/"], "article": r"/(.*presidentielle|.*intentions|wp-content/uploads/.*\.pdf)"},
    {"institute": "Ipsos", "listing": ["https://www.ipsos.com/fr-fr/search?keys=intentions%20de%20vote%20pr%C3%A9sidentielle%202027"], "article": r"presidentielle|intentions[- ]de[- ]vote"},
    {"institute": "Odoxa", "listing": ["https://www.odoxa.fr/sondages/politique/"], "article": r"presidentielle|premier tour|intentions de vote"},
    {"institute": "Verian", "listing": ["https://www.veriangroup.com/fr/news-and-insights/page/1"], "article": r"(presidentielle|barometre)"},
    {"institute": "YouGov", "listing": ["https://yougov.com/fr-fr/articles?category=politics"], "article": r"presidentielle|intentions[- ]de[- ]vote"},
    {"institute": "BVA", "listing": ["https://www.bva-xsight.com/?s=pr%C3%A9sidentielle"], "article": r"(presidentielle|intentions-de-vote)"},
    {"institute": "CSA", "listing": ["https://csa.eu/news/"], "article": r"presidentielle|intentions[- ]de[- ]vote"},
    {"institute": "Viavoice", "listing": ["https://www.institut-viavoice.com/publications/"], "article": r"presidentielle|intentions[- ]de[- ]vote"},
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
    year = None
    for heading, row in re.findall(r"<h[234]\b[^>]*>(.*?)</h[234]>|<tr[^>]*>(.*?)</tr>", page, re.S):
        if heading:
            match = re.search(r"\b(20\d\d)\s*$", text_of(heading))
            if match:
                year = match.group(1)
            continue
        cells = [text_of(c) for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
        if cells:
            header = re.fullmatch(r"(?:[A-Za-zÀ-ÿ]+ )?(20\d\d)", cells[0])
            if header:
                year = header.group(1)
                cells.pop(0)
        if len(cells) < 3:
            continue
        inst = None
        for c in cells[:2]:
            inst = institute_of(c.rsplit(">", 1)[-1]) or inst
        dates = None
        for c in cells[:4]:
            try:
                dates = _wiki_dates(c) or (_wiki_dates(f"{c} {year}") if year else None)
            except ValueError:
                continue
            if dates:
                break
        if not inst or not dates:
            continue
        sample = next((int(re.sub(r"\D", "", c)) for c in cells[1:5] if re.fullmatch(r"\d{1,3}(?:[  ]\d{3})*|\d{3,6}", c) and 300 <= int(re.sub(r"\D", "", c)) <= 50000), None)
        key = (inst, dates[1].isoformat())
        if key in seen:
            continue
        seen.add(key)
        out.append({"institute": inst, "start": dates[0].isoformat(), "end": dates[1].isoformat(), "sample": sample})
    return out
