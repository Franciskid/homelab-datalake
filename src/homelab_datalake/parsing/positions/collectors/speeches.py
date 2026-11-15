# Vendored from the politico repository (scripts/positions/collectors/speeches.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Speeches, interviews and statements from vie-publique.fr ("Discours publics", DILA).

Bulk first: DILA publishes the metadata of every item (~154,000 since 1959) as one JSON file on
echanges.dila.gouv.fr (data.gouv.fr dataset "Métadonnées des Discours publics de
Vie-publique.fr"). It has no text, so the text of each selected item is then read from its
vie-publique.fr page (allowed by robots.txt), one request every few seconds, cached on disk.

Attribution: an item is kept as the person's own words only when their full name appears in the
title; otherwise it is stored with attribution 'douteuse' and not fetched (the metadata has a
few mis-tagged speakers, e.g. a 1988 Jean-Marie Le Pen interview tagged "Marine Le Pen").
"""
from __future__ import annotations

import json
import re
import urllib.parse
from html.parser import HTMLParser

from common import LICENCES, RAW, FetchError, log, norm_name, now, record_raw_file, rel, sha256_file

METADATA = 'https://echanges.dila.gouv.fr/OPENDATA/DISCOURS_PUBLICS/vp_discours.json'
FOLDER = RAW / 'vie-publique'


class TextExtractor(HTMLParser):
    """Collects the text of the first element whose class contains `marker`, keeping paragraphs."""
    BLOCK = {'p', 'div', 'br', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'tr', 'blockquote', 'section'}
    VOID = {'br', 'img', 'hr', 'input', 'meta', 'link', 'source', 'wbr', 'col', 'area', 'base', 'embed', 'param', 'track'}

    def __init__(self, marker: str):
        super().__init__(convert_charrefs=True)
        self.marker = marker
        self.depth = 0          # >0 while inside the wanted element
        self.done = False
        self.parts: list[str] = []
        self.skip = 0           # inside <script>/<style>

    def handle_starttag(self, tag, attrs):
        if self.done:
            return
        if self.depth == 0:
            cls = dict(attrs).get('class') or ''
            if self.marker in cls.split() or self.marker in cls:
                self.depth = 1
            return
        if tag in ('script', 'style'):
            self.skip += 1
        if tag not in self.VOID:
            self.depth += 1
        if tag in self.BLOCK:
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if self.depth == 0 or self.done or tag in self.VOID:
            return
        if tag in ('script', 'style') and self.skip:
            self.skip -= 1
        self.depth -= 1
        if tag in self.BLOCK:
            self.parts.append('\n')
        if self.depth == 0:
            self.done = True

    def handle_data(self, data):
        if self.depth and not self.done and not self.skip:
            self.parts.append(data)

    def text(self) -> str:
        raw = ''.join(self.parts).replace('\r', '')
        lines = [re.sub(r'[ \t ]+', ' ', l).strip() for l in raw.split('\n')]
        out = re.sub(r'\n{3,}', '\n\n', '\n'.join(lines)).strip()
        return out


def extract_text(html: str) -> str:
    p = TextExtractor('field--name-field-texte-integral')
    p.feed(html)
    return p.text()


def clean_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))


def load_metadata(fetcher, conn, refresh: bool):
    path = FOLDER / 'vp_discours.json'
    meta = fetcher.download(METADATA, path, refresh=refresh, stream=True)
    record_raw_file(conn, meta, LICENCES['vie-publique-meta'])
    return json.loads(path.read_text()), meta


def select(items: list[dict], people: list[dict]) -> list[tuple[dict, dict]]:
    """(item, person) pairs where the person is among the item's speakers."""
    by_name = {norm_name(p['name']): p for p in people}
    out = []
    for it in items:
        seen = set()
        for i in it.get('intervenants') or []:
            p = by_name.get(norm_name(i.get('nom')))
            if p and p['id'] not in seen:
                seen.add(p['id'])
                out.append((it, p))
    return out


def attribution(item: dict, person: dict) -> str:
    return 'personnelle' if norm_name(person['name']) in norm_name(item.get('titre')) else 'douteuse'


def item_id(item: dict) -> str | None:
    """The numeric prefix of the page URL (/discours/305316-slug): unique, unlike the metadata
    `id`, which is missing on ~200 items and duplicated on ~100."""
    m = re.search(r'/discours/(\d+)', item.get('url') or '')
    return m.group(1) if m else None


def source_type(item: dict) -> str:
    title = norm_name(item.get('titre'))
    return 'programme' if item.get('type_document') == 'Littérature grise' and title.startswith('programme') else 'speech'


KEY_RE = re.compile(r"voeux|vœux|investiture|prise de fonction|passation|conf[ée]rence de presse|d[ée]claration de politique g[ée]n[ée]rale|"
                    r"message aux fran[çc]ais|lettre aux fran[çc]ais|adresse aux fran[çc]ais|au parlement|congr[èe]s|d[ée]claration (?:télévisée|aux fran)|"
                    r"allocution (?:t[ée]l[ée]vis[ée]e|radiot)|entretien accord[ée]|interview|meeting|discours (?:de|sur) (?:politique|la pol)|"
                    r"programme|projet pr[ée]sidentiel|candidat", re.I)
NOISE_RE = re.compile(r"f[ée]licitations|condol[ée]ances|lettres? de cr[ée]ance|toast|d[iî]ner|d[ée]jeuner|remise de|inauguration|cérémonie|c[ée]r[ée]monie|"
                      r"hommage|obs[èe]ques|anniversaire|message de soutien", re.I)


PRIORITY_RE = re.compile(r"programme [ée]lectoral|profession de foi|candidat[^,.]{0,40}[ée]lection pr[ée]sidentielle|investiture|lettre aux fran[çc]ais|"
                         r"d[ée]claration de candidature|passation|projet (?:pour|pr[ée]sidentiel)|engagements", re.I)


def is_priority(title: str) -> bool:
    return bool(PRIORITY_RE.search(title or '')) and not NOISE_RE.search(title or '')


def is_key(title: str) -> bool:
    return bool(KEY_RE.search(title or '')) and not NOISE_RE.search(title or '')


def run(conn, fetcher, person_ids: list[str], refresh: bool = False, limit: int | None = None, fetch_text: bool = True,
        per_person: int | None = None, key_only: bool = False) -> dict:
    items, _ = load_metadata(fetcher, conn, refresh)
    people = [dict(r) for r in conn.execute(f"SELECT id, name FROM people WHERE id IN ({','.join('?' * len(person_ids))})", person_ids)]
    pairs = select(items, people)
    stats = {'metadata_items': len(items), 'per_person': {pid: {'listed': 0, 'doubtful': 0, 'fetched': 0, 'failed': 0, 'empty': 0, 'chars': 0} for pid in person_ids}}
    conn.execute('BEGIN')
    for it, p in pairs:
        if not item_id(it):
            stats['no_id'] = stats.get('no_id', 0) + 1
            continue
        conn.execute("""INSERT OR IGNORE INTO person_external_ids (person, source, external_id, external_name, method, matched_at)
                        VALUES (?, 'vie-publique', ?, ?, 'exact-name', ?)""", (p['id'], norm_name(p['name']), p['name'], now()))
        attr = attribution(it, p)
        conn.execute(
            """INSERT INTO documents (person, source_type, source, external_id, url, title, date, licence, attribution, status, status_note)
               VALUES (?, ?, 'vie-publique', ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source, external_id, person) DO UPDATE SET title=excluded.title, date=excluded.date,
                 url=excluded.url, attribution=excluded.attribution""",
            (p['id'], source_type(it), item_id(it), clean_url(it['url']), (it.get('titre') or '').strip(), it.get('prononciation'),
             LICENCES['vie-publique-text'], attr, 'pending' if attr == 'personnelle' else 'skipped',
             None if attr == 'personnelle' else 'nom complet absent du titre : attribution à vérifier'))
        stats['per_person'][p['id']]['listed'] += 1
        stats['per_person'][p['id']]['doubtful'] += attr == 'douteuse'
    conn.execute('COMMIT')
    if not fetch_text:
        return stats
    todo = conn.execute(
        f"""SELECT id, person, external_id, url FROM documents WHERE source = 'vie-publique' AND status IN ('pending', 'failed')
            AND person IN ({','.join('?' * len(person_ids))}) ORDER BY date DESC""", person_ids).fetchall()
    if key_only:
        titles = {r['id']: r['title'] for r in conn.execute("SELECT id, title FROM documents WHERE source = 'vie-publique'")}
        todo = [d for d in todo if is_key(titles.get(d['id']))]
        todo.sort(key=lambda d: not is_priority(titles.get(d['id'])))  # stable: campaign documents first, then most recent
    if per_person:
        seen: dict[str, int] = {}
        kept = []
        for d in todo:
            seen[d['person']] = seen.get(d['person'], 0) + 1
            if seen[d['person']] <= per_person:
                kept.append(d)
        todo = kept
    if limit:
        todo = todo[:limit]
    for n, d in enumerate(todo, 1):
        html_path = FOLDER / 'pages' / f"{d['external_id']}.html"
        txt_path = FOLDER / 'text' / f"{d['external_id']}.txt"
        try:
            meta = fetcher.download(d['url'], html_path)
        except FetchError as e:
            conn.execute("UPDATE documents SET status='failed', status_note=? WHERE id=?", (str(e)[:300], d['id']))
            stats['per_person'][d['person']]['failed'] += 1
            log(f'  vie-publique: failed {d["url"]}: {e}')
            continue
        raw_id = record_raw_file(conn, meta, LICENCES['vie-publique-text'])
        text = extract_text(html_path.read_text(encoding='utf-8', errors='replace'))
        if len(text) < 200:
            conn.execute("UPDATE documents SET status='failed', status_note=?, fetched_at=?, raw_file=? WHERE id=?",
                         (f'texte intégral introuvable ou trop court ({len(text)} caractères)', meta['fetched_at'], raw_id, d['id']))
            stats['per_person'][d['person']]['empty'] += 1
            continue
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        txt_path.write_text(text, encoding='utf-8')
        conn.execute("UPDATE documents SET status='fetched', status_note=NULL, fetched_at=?, sha256=?, path=?, chars=?, raw_file=? WHERE id=?",
                     (meta['fetched_at'], sha256_file(txt_path), rel(txt_path), len(text), raw_id, d['id']))
        stats['per_person'][d['person']]['fetched'] += 1
        stats['per_person'][d['person']]['chars'] += len(text)
        if n % 25 == 0:
            log(f'  vie-publique: {n}/{len(todo)}')
    return stats
