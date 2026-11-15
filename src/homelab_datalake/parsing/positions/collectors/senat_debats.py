# Vendored from the politico repository (scripts/positions/collectors/senat_debats.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Sénat debate transcripts (data.senat.fr "Comptes rendus", cri.zip: one XML per sitting day).

Same output as an_debats: one document per person and sitting day, the person's own
interventions only (`<cri:intervenant mat="…">` blocks, matched on the senator's matricule),
with `document_turns` offsets. The speaker label and parenthetical stage directions are removed.
"""
from __future__ import annotations

import re
import zipfile
from html import unescape

from collectors.an_debats import MIN_TURN_CHARS, Turn, build_document
from common import LICENCES, RAW, log, record_raw_file, rel, sha256_file

URL = 'https://data.senat.fr/data/debats/cri.zip'
FOLDER = RAW / 'senat-debats'


_TITLE = re.compile(r'<cri:titreS\d+(?!bdc)[^>]*?\blibelle="([^"]*)"')
_BLOCK = re.compile(r'<cri:intervenant\b[^>]*?\bmat="([^"]*)"[^>]*>(.*?)</cri:intervenant>', re.S)
_PARA = re.compile(r'<p\b[^>]*\bid="par_\d+"[^>]*>(.*?)</p>', re.S)
_DROP = re.compile(r'<cri:orateurnom>.*?</cri:orateurnom>|<cri:infoentreparentheses>.*?</cri:infoentreparentheses>', re.S)


def para_text(html: str) -> str:
    """One <p> body without the speaker label and stage directions, tags stripped."""
    t = _DROP.sub(' ', html)
    t = re.sub(r'<br\s*/?>', ' ', t)
    t = re.sub(r'<[^>]+>', '', t)
    return re.sub(r'\s+', ' ', unescape(t)).strip()


def parse_day(xml: bytes, wanted: set[str]) -> list[tuple[str, Turn]]:
    """[(matricule, Turn)] for the wanted senators in one sitting day. The files are not always
    well-formed XML (stray <br>), so this reads them with regular expressions."""
    text = xml.decode('iso-8859-1', 'replace')
    events = sorted([(m.start(), 'title', m) for m in _TITLE.finditer(text)] +
                    [(m.start(), 'block', m) for m in _BLOCK.finditer(text)], key=lambda e: e[0])
    out: list[tuple[str, Turn]] = []
    context = ''
    last: tuple[str, Turn] | None = None
    for _, kind, m in events:
        if kind == 'title':
            context = unescape(m.group(1)).strip()
            last = None
            continue
        mat = m.group(1)
        if mat not in wanted:
            last = None
            continue
        body = '\n'.join(t for t in (para_text(p) for p in _PARA.findall(m.group(2))) if t).strip()
        if not body:
            continue
        if last and last[0] == mat and last[1].context == context:
            last[1].text += '\n' + body
        else:
            last = (mat, Turn(mat, context, body))
            out.append(last)
    return out


def run(conn, fetcher, person_ids: list[str], refresh: bool = False) -> dict:
    mats = {r['external_id']: r['person'] for r in conn.execute(
        f"SELECT external_id, person FROM person_external_ids WHERE source='senat' AND person IN ({','.join('?' * len(person_ids))})", person_ids)}
    stats = {'days': 0, 'per_person': {pid: {'documents': 0, 'turns': 0, 'short_turns_dropped': 0, 'chars': 0, 'first': None, 'last': None} for pid in mats.values()}, 'matched': mats}
    if not mats:
        return stats
    dest = FOLDER / 'cri.zip'
    meta = fetcher.download(URL, dest, refresh=refresh, stream=True)
    raw_id = record_raw_file(conn, meta, LICENCES['senat'])
    wanted_bytes = [m.encode() for m in mats]
    with zipfile.ZipFile(dest) as z:
        for name in sorted(z.namelist()):
            m = re.search(r'cri/d(\d{8})\.xml$', name)
            if not m:
                continue
            raw = z.read(name)
            stats['days'] += 1
            if not any(b'mat="' + w + b'"' in raw for w in wanted_bytes):
                continue
            day = m.group(1)
            date = f'{day[:4]}-{day[4:6]}-{day[6:]}'
            by_person: dict[str, list[Turn]] = {}
            for mat, turn in parse_day(raw, set(mats)):
                pid = mats[mat]
                if len(turn.text) < MIN_TURN_CHARS:
                    stats['per_person'][pid]['short_turns_dropped'] += 1
                else:
                    by_person.setdefault(pid, []).append(turn)
            for pid, ts in by_person.items():
                text, spans = build_document({}, ts)
                path = FOLDER / 'text' / pid / f'd{day}.txt'
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding='utf-8')
                conn.execute('BEGIN')
                conn.execute(
                    """INSERT INTO documents (person, source_type, source, external_id, url, title, date, fetched_at, sha256, path, licence,
                                              attribution, status, chars, raw_file)
                       VALUES (?, 'speech', 'senat-debats', ?, ?, ?, ?, ?, ?, ?, ?, 'personnelle', 'fetched', ?, ?)
                       ON CONFLICT(source, external_id, person) DO UPDATE SET title=excluded.title, date=excluded.date, sha256=excluded.sha256,
                         path=excluded.path, chars=excluded.chars, raw_file=excluded.raw_file, status='fetched'""",
                    (pid, f'd{day}', f'{URL}#d{day}', f'Sénat — compte rendu intégral du {date} : interventions de {pid}', date,
                     meta['fetched_at'], sha256_file(path), rel(path), LICENCES['senat'], len(text), raw_id))
                doc = conn.execute("SELECT id FROM documents WHERE source='senat-debats' AND external_id=? AND person=?", (f'd{day}', pid)).fetchone()[0]
                conn.execute('DELETE FROM document_turns WHERE document = ?', (doc,))
                conn.executemany('INSERT INTO document_turns (document, person, char_start, char_end, context) VALUES (?,?,?,?,?)',
                                 [(doc, pid, a, b, c) for a, b, c in spans])
                conn.execute('COMMIT')
                s = stats['per_person'][pid]
                s['documents'] += 1; s['turns'] += len(spans); s['chars'] += sum(b - a for a, b, _ in spans)
                s['first'] = min(s['first'] or date, date); s['last'] = max(s['last'] or date, date)
    log(f"  Sénat débats: {stats['days']} sitting days read")
    return stats
