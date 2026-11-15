# Vendored from the politico repository (scripts/positions/collectors/an_debats.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Assemblée nationale debate transcripts ("comptes rendus intégraux", open data "syceron brut").

One bulk zip per legislature (XV, XVI, XVII; XIV has no open-data export). Each XML is one
sitting. For each tracked deputy (matched earlier by votes-an: person_external_ids source 'an')
we keep his or her own interventions only: consecutive paragraphs with the same `id_acteur`
under the same agenda point are one turn. A document = one person in one sitting; its text file
holds the turns separated by header lines ("## <agenda point>") and `document_turns` stores the
character span of each turn, so extraction never reads anyone else's words nor the headers.
Stage directions (italic parentheses such as "(Applaudissements…)") are removed.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass

from common import LICENCES, RAW, log, now, record_raw_file, rel, sha256_file

URL = 'https://data.assemblee-nationale.fr/static/openData/repository/{leg}/vp/syceronbrut/syseron.xml.zip'
LEGISLATURES = (15, 16, 17)
FOLDER = RAW / 'an-debats'
MIN_TURN_CHARS = 200          # shorter interventions ("Je vous remercie, madame la présidente") are not positions
NS = '{http://schemas.assemblee-nationale.fr/referentiel}'


@dataclass
class Turn:
    actor: str
    context: str
    text: str


def clean(el) -> str:
    """Text of a <texte> element without italic stage directions; <br/> becomes a space."""
    parts = []

    def walk(e):
        if e.tag == NS + 'italique':
            if e.tail:
                parts.append(e.tail)
            return
        if e.text:
            parts.append(e.text)
        for c in e:
            if c.tag == NS + 'br':
                parts.append(' ')
                if c.tail:
                    parts.append(c.tail)
            else:
                walk(c)
        if e is not el and e.tail:
            parts.append(e.tail)
    walk(el)
    return re.sub(r'\s+', ' ', ''.join(parts)).strip()


def plain(el) -> str:
    return re.sub(r'\s+', ' ', ''.join(el.itertext())).strip()


def parse_sitting(xml: bytes) -> tuple[dict, list[Turn]]:
    root = ET.fromstring(xml)
    meta = root.find(NS + 'metadonnees')
    date = (meta.findtext(NS + 'dateSeance') or '')[:8]
    info = {'uid': root.findtext(NS + 'uid'), 'date': f'{date[:4]}-{date[4:6]}-{date[6:]}' if len(date) == 8 else None,
            'day': meta.findtext(NS + 'dateSeanceJour')}
    contenu = root.find(NS + 'contenu')
    turns: list[Turn] = []
    context = ''
    cur: Turn | None = None
    for el in contenu.iter():
        if el.tag in (NS + 'point', NS + 'ouvertureSeance'):
            t = el.find(NS + 'texte')
            if t is not None and el.tag == NS + 'point':
                context = plain(t)
            cur = None if el.tag == NS + 'point' else cur
        elif el.tag == NS + 'paragraphe':
            actor = el.get('id_acteur')
            t = el.find(NS + 'texte')
            text = clean(t) if t is not None else ''
            if not actor or not text:
                if actor is None and text:
                    cur = None
                continue
            if cur and cur.actor == actor and cur.context == context:
                cur.text += '\n' + text
            else:
                cur = Turn(actor, context, text)
                turns.append(cur)
    return info, turns


def build_document(info: dict, turns: list[Turn]) -> tuple[str, list[tuple[int, int, str]]]:
    """(text, [(start, end, context)]) for one person's turns in one sitting."""
    out, spans, pos = [], [], 0
    last_ctx = None
    for t in turns:
        if t.context != last_ctx:
            head = f'## {t.context or "Séance"}\n'
            out.append(head)
            pos += len(head)
            last_ctx = t.context
        out.append(t.text)
        spans.append((pos, pos + len(t.text), t.context))
        pos += len(t.text)
        out.append('\n\n')
        pos += 2
    return ''.join(out).rstrip('\n') + '\n', spans


def run(conn, fetcher, person_ids: list[str], refresh: bool = False, legislatures=LEGISLATURES) -> dict:
    actors = {r['external_id']: r['person'] for r in conn.execute(
        f"SELECT external_id, person FROM person_external_ids WHERE source='an' AND person IN ({','.join('?' * len(person_ids))})", person_ids)}
    stats = {'sittings': 0, 'per_person': {pid: {'documents': 0, 'turns': 0, 'short_turns_dropped': 0, 'chars': 0} for pid in person_ids}, 'matched': actors}
    if not actors:
        return stats
    for leg in legislatures:
        dest = FOLDER / str(leg) / 'syseron.xml.zip'
        meta = fetcher.download(URL.format(leg=leg), dest, refresh=refresh, stream=True)
        raw_id = record_raw_file(conn, meta, LICENCES['an'])
        n = 0
        with zipfile.ZipFile(dest) as z:
            for name in sorted(z.namelist()):
                if not name.endswith('.xml'):
                    continue
                raw = z.read(name)
                if not any(a.encode() in raw for a in actors):     # cheap pre-filter on the actor ids
                    stats['sittings'] += 1
                    continue
                info, turns = parse_sitting(raw)
                stats['sittings'] += 1
                by_person: dict[str, list[Turn]] = {}
                for t in turns:
                    pid = actors.get(f'PA{t.actor[2:]}' if not t.actor.startswith('PA') else t.actor)
                    if pid:
                        if len(t.text) < MIN_TURN_CHARS:
                            stats['per_person'][pid]['short_turns_dropped'] += 1
                        else:
                            by_person.setdefault(pid, []).append(t)
                for pid, ts in by_person.items():
                    text, spans = build_document(info, ts)
                    path = FOLDER / 'text' / pid / f"{info['uid']}.txt"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(text, encoding='utf-8')
                    title = f"Assemblée nationale — séance du {info['day'] or info['date']} : interventions de {pid}"
                    conn.execute('BEGIN')
                    conn.execute(
                        """INSERT INTO documents (person, source_type, source, external_id, url, title, date, fetched_at, sha256, path, licence,
                                                  attribution, status, chars, raw_file)
                           VALUES (?, 'speech', 'an-debats', ?, ?, ?, ?, ?, ?, ?, ?, 'personnelle', 'fetched', ?, ?)
                           ON CONFLICT(source, external_id, person) DO UPDATE SET title=excluded.title, date=excluded.date, sha256=excluded.sha256,
                             path=excluded.path, chars=excluded.chars, raw_file=excluded.raw_file, status='fetched'""",
                        (pid, info['uid'], f"{URL.format(leg=leg)}#{info['uid']}", title, info['date'], meta['fetched_at'],
                         sha256_file(path), rel(path), LICENCES['an'], len(text), raw_id))
                    doc = conn.execute("SELECT id FROM documents WHERE source='an-debats' AND external_id=? AND person=?", (info['uid'], pid)).fetchone()[0]
                    conn.execute('DELETE FROM document_turns WHERE document = ?', (doc,))
                    conn.executemany('INSERT INTO document_turns (document, person, char_start, char_end, context) VALUES (?,?,?,?,?)',
                                     [(doc, pid, a, b, c) for a, b, c in spans])
                    conn.execute('COMMIT')
                    s = stats['per_person'][pid]
                    s['documents'] += 1; s['turns'] += len(spans); s['chars'] += sum(b - a for a, b, _ in spans)
                    n += 1
        log(f'  AN débats legislature {leg}: {n} person-sittings')
    return stats
