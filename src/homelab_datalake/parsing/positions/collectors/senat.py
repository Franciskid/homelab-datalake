# Vendored from the politico repository (scripts/positions/collectors/senat.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Sénat public ballots (data.senat.fr).

The "dosleg" bulk file is a PostgreSQL dump; its `scr` table holds the ballots (since the
2006–2007 session) and `votsen` each senator's vote. Senators come from ODSEN_GENERAL.json.
We read the COPY blocks straight from the zip, no database server needed.
"""
from __future__ import annotations

import io
import json
import zipfile

from common import LICENCES, RAW, log, record_raw_file
from match import Member, load_people, match_members, pins_for, store
from collectors.an import upsert_ballot, upsert_vote

DOSLEG = 'https://data.senat.fr/data/dosleg/dosleg.zip'
SENATEURS = 'https://data.senat.fr/data/senateurs/ODSEN_GENERAL.json'
POSITIONS = {'1': 'pour', '2': 'contre', '3': 'abstention', '4': 'non-votant'}

_ESC = {'t': '\t', 'n': '\n', 'r': '\r', '\\': '\\', 'b': '\b', 'f': '\f', 'v': '\v'}


def unescape_copy(field: str) -> str | None:
    """PostgreSQL COPY text format: \\N is NULL, backslash escapes otherwise."""
    if field == '\\N':
        return None
    if '\\' not in field:
        return field
    out, i = [], 0
    while i < len(field):
        c = field[i]
        if c == '\\' and i + 1 < len(field):
            out.append(_ESC.get(field[i + 1], field[i + 1]))
            i += 2
        else:
            out.append(c)
            i += 1
    return ''.join(out)


def copy_blocks(lines, tables: set[str]):
    """Yields (table, columns, [values]) for every row of the wanted COPY blocks."""
    current = None
    for line in lines:
        if current is None:
            if line.startswith('COPY '):
                name = line.split()[1]
                if name in tables:
                    cols = line[line.index('(') + 1:line.index(')')].split(', ')
                    current = (name, cols)
            continue
        if line.rstrip('\n') == '\\.':
            current = None
            continue
        yield current[0], current[1], [unescape_copy(f) for f in line.rstrip('\n').split('\t')]


def parse_senateurs(path) -> list[Member]:
    data = json.loads(path.read_text())
    out = []
    for s in data['results']:
        born = s.get('Date_naissance')
        out.append(Member(s['Matricule'], s['Prenom_usuel'], s['Nom_usuel'], int(born[:4]) if born else None))
    return out


def ballot_row(r: dict) -> dict:
    ses, num = r['sesann'], r['scrnum']
    title = (r['scrint'] or '').strip()
    return {
        'id': f'senat-{ses}-{num}', 'chamber': 'Senat', 'term': ses, 'number': num,
        'date': (r['scrdat'] or '')[:10], 'title': title, 'object': title, 'vote_type': None,
        'result': None,
        'n_for': int(r['scrpou']) if r['scrpou'] else None,
        'n_against': int(r['scrcon']) if r['scrcon'] else None,
        'n_abstain': None,
        'url': f'https://www.senat.fr/scrutin-public/{ses}/scr{ses}-{num}.html',
        'extra': json.dumps({'texte_code': r['code'], 'suffrages': r['scrsuf'], 'majorite': r['scrmaj']}),
    }


def run(conn, fetcher, person_ids: list[str], refresh: bool = False, config: dict | None = None) -> dict:
    sen_path = RAW / 'senat' / 'ODSEN_GENERAL.json'
    record_raw_file(conn, fetcher.download(SENATEURS, sen_path, refresh=refresh, stream=True), LICENCES['senat'])
    members = parse_senateurs(sen_path)
    report = match_members(load_people(conn), members, pins_for(config or {}, 'senat'))
    store(conn, 'senat', report, members)
    for pid, ids in report.ambiguous.items():
        log(f'  Sénat: ambiguous {pid}: {ids}')
    wanted = {ext: pid for ext, (pid, _) in report.matched.items() if pid in person_ids}
    zpath = RAW / 'senat' / 'dosleg.zip'
    raw_id = record_raw_file(conn, fetcher.download(DOSLEG, zpath, refresh=refresh, stream=True), LICENCES['senat'])
    stats = {'members': len(members), 'matched': {pid: ext for ext, pid in wanted.items()},
             'ballots': 0, 'votes': {pid: 0 for pid in person_ids}, 'votes_skipped_no_ballot': 0}
    conn.execute('BEGIN')
    with zipfile.ZipFile(zpath) as z, z.open('dosleg.sql') as f:
        lines = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
        known = set()
        # The dump lists `scr` before `votsen`; check rather than assume.
        for table, cols, values in copy_blocks(lines, {'scr', 'votsen'}):
            r = dict(zip(cols, values))
            if table == 'scr':
                b = ballot_row(r)
                upsert_ballot(conn, b, raw_id)
                known.add(b['id'])
                stats['ballots'] += 1
            else:
                pid = wanted.get(r['senmat'])
                if not pid:
                    continue
                bid = f"senat-{r['sesann']}-{r['scrnum']}"
                if bid not in known:
                    stats['votes_skipped_no_ballot'] += 1
                    continue
                position = POSITIONS.get(r['posvotcod'] or '4', 'non-votant')
                upsert_vote(conn, bid, pid, position, None, 1 if r.get('senmatdel') else 0)
                stats['votes'][pid] += 1
    conn.execute('COMMIT')
    log(f"  Sénat: {stats['ballots']} ballots")
    return stats
