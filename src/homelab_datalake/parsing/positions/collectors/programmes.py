# Vendored from the politico repository (scripts/positions/collectors/programmes.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Programmes and platforms listed in scripts/positions/programmes.json (official PDFs / pages).

Each entry is downloaded (cached, robots-checked, rate-limited), converted to plain text
(PDF via poppler's `pdftotext`, HTML via the stdlib parser) and stored as a `programme`
document. A failure is recorded on the document (status 'failed') — never silently dropped.
Documents that vie-publique.fr serves as "Programme …" are collected by speeches.py instead.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from html.parser import HTMLParser

from common import HERE, RAW, FetchError, LICENCES, log, now, record_raw_file, rel, sha256_file

MANIFEST = HERE / 'programmes.json'
FOLDER = RAW / 'programmes'
LICENCES['programme'] = 'Document publié par son auteur (candidat ou parti) ; droits réservés, usage de citation'


class PageText(HTMLParser):
    """Visible text of <article> (else <main>, else <body>), block-structured."""
    BLOCK = {'p', 'div', 'br', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'tr', 'blockquote', 'section'}
    SKIP = {'script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'noscript', 'svg'}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = {'article': [], 'main': [], 'body': []}
        self.open = {'article': 0, 'main': 0, 'body': 0}
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP:
            self.skip += 1
        if tag in self.open:
            self.open[tag] += 1
        if tag in self.BLOCK:
            for k in self.open:
                if self.open[k]:
                    self.parts[k].append('\n')

    def handle_endtag(self, tag):
        if tag in self.SKIP and self.skip:
            self.skip -= 1
        if tag in self.open and self.open[tag]:
            self.open[tag] -= 1
        if tag in self.BLOCK:
            for k in self.open:
                if self.open[k]:
                    self.parts[k].append('\n')

    def handle_data(self, data):
        if self.skip:
            return
        for k in self.open:
            if self.open[k]:
                self.parts[k].append(data)

    def text(self) -> str:
        for k in ('article', 'main', 'body'):
            raw = ''.join(self.parts[k])
            if len(raw.strip()) > 200:
                break
        lines = [re.sub(r'[ \t ]+', ' ', l).strip() for l in raw.replace('\r', '').split('\n')]
        return re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip()


def html_to_text(html: str) -> str:
    p = PageText()
    p.feed(html)
    return p.text()


def pdf_to_text(path) -> str:
    exe = shutil.which('pdftotext')
    if not exe:
        raise FetchError('pdftotext (poppler) is not installed: brew install poppler')
    out = subprocess.run([exe, '-enc', 'UTF-8', '-nopgbrk', str(path), '-'], capture_output=True, timeout=300)
    if out.returncode != 0:
        raise FetchError(f'pdftotext failed: {out.stderr.decode("utf-8", "replace")[:200]}')
    text = out.stdout.decode('utf-8', 'replace').replace('\f', '\n')
    # Join words hyphenated at line ends, keep paragraph breaks.
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()


_STAMP = re.compile(r'^\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}|.*\.indd\s+\d+)\s*$')


def spaced_out(line: str) -> bool:
    """'É L E C T I O N  P RÉ S I D E N T I E L L E': letter-spaced display type in PDFs."""
    toks = line.split()
    return len(toks) >= 6 and sum(len(t) <= 2 for t in toks) / len(toks) > 0.7


def clean_pdf_text(text: str, start_marker: str | None = None) -> str:
    """Removes page furniture from pdftotext output and reflows hard-wrapped lines.

    - everything before `start_marker` (cover overprints) is dropped when given;
    - short lines repeated 4+ times (running headers / footers), letter-spaced titles and
      export stamps (file name, date-time) are dropped;
    - a line that does not end a sentence is joined to the next one (blank lines separate blocks).
    """
    if start_marker and start_marker in text:
        text = text[text.index(start_marker):]
    lines = [l.rstrip() for l in text.split('\n')]
    counts: dict[str, int] = {}
    for l in lines:
        k = l.strip()
        if k:
            counts[k] = counts.get(k, 0) + 1
    kept = [l for l in lines if not l.strip() or not ((counts[l.strip()] >= 4 and len(l.strip()) <= 90) or spaced_out(l) or _STAMP.match(l) or re.fullmatch(r'\s*\d{1,3}\s*', l))]
    out, buf = [], ''
    for l in kept:
        s = l.strip()
        if not s:
            if buf:
                out.append(buf)
                buf = ''
            continue
        if buf and not re.search(r'[.!?:;»”…)]$', buf):
            buf = buf + ' ' + s
        else:
            if buf:
                out.append(buf)
            buf = s
    if buf:
        out.append(buf)
    return re.sub(r'[ \t\u00a0]+', ' ', '\n\n'.join(out)).strip()


def book_chapter_urls(index_html: str, index_url: str) -> list[str]:
    """Ordered chapter URLs of an online book index (melenchon2027.fr/programme/livre)."""
    import urllib.parse
    seen, out = set(), []
    for href in re.findall(r'href=["\']([^"\']*/programme/livre/[^"\'#?]*)["\']', index_html):
        url = urllib.parse.urljoin(index_url, href).rstrip('/')
        if re.search(r'/programme/livre/(introduction|[^/]+/chapitre\d+)$', url) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def book_chapter_text(html: str) -> str:
    m = re.search(r'<main\b[^>]*class="chapitre[^"]*"[^>]*>(.*?)</main>', html, re.S)
    inner = m.group(1) if m else html
    inner = re.sub(r'</?header\b', lambda x: x.group(0).replace('header', 'div'), inner)   # keep the part / chapter titles
    return html_to_text(f'<body>{inner}</body>')


def fetch_entry(fetcher, e: dict, refresh: bool) -> tuple[str, dict, str]:
    """Download one manifest entry and return (plain text, download metadata, URL actually used)."""
    key = e['key']
    if e['format'] == 'html-book':          # an online book: index page + one page per chapter
        index = FOLDER / 'files' / f'{key}-index.html'
        meta = fetcher.download(e['index_url'], index, refresh=refresh)
        urls = book_chapter_urls(index.read_text(encoding='utf-8', errors='replace'), e['index_url'])
        if not urls:
            raise FetchError(f"no chapter links found on {e['index_url']}")
        parts = []
        for url in urls:
            path = FOLDER / 'files' / key / (url.rsplit('/programme/livre/', 1)[1].replace('/', '--') + '.html')
            fetcher.download(url, path, refresh=refresh)
            text = book_chapter_text(path.read_text(encoding='utf-8', errors='replace'))
            if len(text) < 300:
                raise FetchError(f'chapter {url} has no text ({len(text)} chars)')
            parts.append(text)
        return '\n\n'.join(parts), {**meta, 'url': e['url']}, e['url']
    ext = 'pdf' if e['format'] == 'pdf' else 'html'
    dest = FOLDER / 'files' / f'{key}.{ext}'
    used = e['url']
    try:
        meta = fetcher.download(used, dest, refresh=refresh)
    except FetchError as err:
        if not e.get('fallback'):
            raise
        log(f'  programmes: {used} failed ({err}); trying the listed mirror')
        used = e['fallback']
        meta = fetcher.download(used, dest, refresh=refresh)
    if ext == 'pdf' and dest.read_bytes()[:5] != b'%PDF-':
        final = meta.get('final_url')
        dest.unlink()
        dest.with_name(dest.name + '.meta.json').unlink(missing_ok=True)   # do not cache a non-PDF as the PDF
        raise FetchError(f'not a PDF (served {meta.get("content_type")}, final URL {final})')
    text = clean_pdf_text(pdf_to_text(dest), e.get('start_marker')) if ext == 'pdf' else html_to_text(dest.read_text(encoding='utf-8', errors='replace'))
    if len(text) < 500:
        raise FetchError(f'extracted text too short ({len(text)} chars): scanned PDF or script-rendered page?')
    return text, meta, used


def run(conn, fetcher, person_ids: list[str], refresh: bool = False) -> dict:
    manifest = json.loads(MANIFEST.read_text())
    stats = {'listed': 0, 'fetched': 0, 'failed': 0, 'chars': 0, 'failures': []}
    ids = set(person_ids)
    known = {r[0] for r in conn.execute('SELECT id FROM people')}
    for c in manifest['coverage']:
        if c['person'] in ids and c['person'] in known:
            conn.execute("""INSERT INTO coverage_notes (person, source_type, period, status, note, url, checked_at)
                            VALUES (?, 'programme', ?, ?, ?, ?, ?)
                            ON CONFLICT(person, source_type, period) DO UPDATE SET status=excluded.status, note=excluded.note,
                              url=excluded.url, checked_at=excluded.checked_at""",
                         (c['person'], c['period'], c['status'], c.get('note'), c.get('url'), now()))
    for e in manifest['documents']:
        if e['person'] not in ids:
            continue
        stats['listed'] += 1
        conn.execute(
            """INSERT INTO documents (person, source_type, source, external_id, url, title, date, licence, attribution, status)
               VALUES (?, 'programme', 'programme-manifest', ?, ?, ?, ?, ?, ?, 'pending')
               ON CONFLICT(source, external_id, person) DO UPDATE SET url=excluded.url, title=excluded.title""",
            (e['person'], e['key'], e['url'], e['title'], str(e['year']), LICENCES['programme'], e['attribution']))
        doc = conn.execute("SELECT id, status FROM documents WHERE source='programme-manifest' AND external_id=? AND person=?",
                           (e['key'], e['person'])).fetchone()
        try:
            text, meta, used = fetch_entry(fetcher, e, refresh)
        except (FetchError, subprocess.SubprocessError) as err:
            conn.execute("UPDATE documents SET status='failed', status_note=? WHERE id=?", (str(err)[:300], doc['id']))
            stats['failed'] += 1
            stats['failures'].append({'key': e['key'], 'error': str(err)[:200]})
            log(f"  programmes: FAILED {e['key']}: {err}")
            continue
        raw_id = record_raw_file(conn, meta, LICENCES['programme'])
        txt = FOLDER / 'text' / f"{e['key']}.txt"
        txt.parent.mkdir(parents=True, exist_ok=True)
        txt.write_text(text, encoding='utf-8')
        note = None if used == e['url'] else f'copie tierce utilisée : {used}'
        conn.execute("""UPDATE documents SET status='fetched', status_note=?, url=?, fetched_at=?, sha256=?, path=?, chars=?, raw_file=? WHERE id=?""",
                     (note, used, meta['fetched_at'], sha256_file(txt), rel(txt), len(text), raw_id, doc['id']))
        stats['fetched'] += 1
        stats['chars'] += len(text)
        log(f"  programmes: {e['key']} {len(text)} chars")
    return stats
