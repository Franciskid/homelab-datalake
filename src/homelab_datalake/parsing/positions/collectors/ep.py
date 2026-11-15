# Vendored from the politico repository (scripts/positions/collectors/ep.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""European Parliament roll-call votes from the HowTheyVote.eu data export (GitHub releases).

Covers the 9th term (July 2019) onwards: earlier MEP careers (e.g. Marine Le Pen 2004–2017,
Jean-Luc Mélenchon 2009–2017) are not in this export. "DID_NOT_VOTE" rows are absences and are
not stored, like absentees at the Assemblée.
"""
from __future__ import annotations

import csv
import gzip
import json

from common import LICENCES, RAW, log, record_raw_file
from match import Member, load_people, match_members, pins_for, store
from collectors.an import upsert_ballot, upsert_vote

RELEASES = 'https://api.github.com/repos/HowTheyVote/data/releases/latest'
FILES = ('members.csv.gz', 'votes.csv.gz', 'member_votes.csv.gz')
POSITIONS = {'FOR': 'pour', 'AGAINST': 'contre', 'ABSTENTION': 'abstention'}


def latest_release(fetcher) -> tuple[str, dict[str, str]]:
    """(tag, {file name: API asset URL}). github.com's robots.txt disallows /*/*/download/* for crawlers, so the
    assets are fetched through the GitHub REST API (Accept: application/octet-stream), which has no robots file and is
    the documented way to download release assets programmatically."""
    data, _ = fetcher.get_bytes(RELEASES)
    rel = json.loads(data)
    return rel['tag_name'], {a['name']: a['url'] for a in rel['assets']}


def read_csv_gz(path):
    with gzip.open(path, 'rt', encoding='utf-8', newline='') as f:
        yield from csv.DictReader(f)


def ballot_row(r: dict) -> dict:
    title = r['display_title'] or r['procedure_title'] or r['reference'] or f"vote {r['id']}"
    detail = ' · '.join(x for x in (r['description'], r['amendment_subject']) if x)
    return {
        'id': f"ep-{r['id']}", 'chamber': 'PE', 'term': None, 'number': r['id'],
        'date': r['timestamp'][:10], 'title': title, 'object': detail or None,
        'vote_type': 'main' if r['is_main'] == 'True' else 'amendment-or-part',
        'result': r['result'] or None,
        'n_for': int(r['count_for']) if r['count_for'] else None,
        'n_against': int(r['count_against']) if r['count_against'] else None,
        'n_abstain': int(r['count_abstention']) if r['count_abstention'] else None,
        'url': f"https://howtheyvote.eu/votes/{r['id']}",
        'extra': json.dumps({k: r[k] for k in ('reference', 'procedure_reference', 'procedure_type', 'amendment_number', 'texts_adopted_reference') if r.get(k)}),
    }


def run(conn, fetcher, person_ids: list[str], refresh: bool = False, release: str | None = None, config: dict | None = None) -> dict:
    assets = {}
    if release is None:
        release, assets = latest_release(fetcher)
    folder = RAW / 'ep' / release
    paths = {}
    for name in FILES:
        url = f'https://github.com/HowTheyVote/data/releases/download/{release}/{name}'
        paths[name] = folder / name
        if name in assets:
            meta = fetcher.download(assets[name], folder / name, refresh=refresh, stream=True, robots=False,
                                    headers={'Accept': 'application/octet-stream'}, canonical_url=url)
        else:
            meta = fetcher.download(url, folder / name, refresh=refresh, stream=True)
        record_raw_file(conn, meta, LICENCES['ep'])
    raw_id = conn.execute('SELECT id FROM raw_files WHERE url LIKE ?', (f'%/{release}/votes.csv.gz',)).fetchone()[0]
    members = [Member(r['id'], r['first_name'], r['last_name'], int(r['date_of_birth'][:4]) if r['date_of_birth'] else None)
               for r in read_csv_gz(paths['members.csv.gz']) if r['country_code'] == 'FRA']
    report = match_members(load_people(conn), members, pins_for(config or {}, 'ep'))
    store(conn, 'ep', report, members)
    for pid, ids in report.ambiguous.items():
        log(f'  PE: ambiguous {pid}: {ids}')
    wanted = {ext: pid for ext, (pid, _) in report.matched.items() if pid in person_ids}
    stats = {'release': release, 'members_fr': len(members), 'matched': {pid: ext for ext, pid in wanted.items()},
             'ballots': 0, 'votes': {pid: 0 for pid in person_ids}, 'absences': {pid: 0 for pid in person_ids}}
    conn.execute('BEGIN')
    for r in read_csv_gz(paths['votes.csv.gz']):
        upsert_ballot(conn, ballot_row(r), raw_id)
        stats['ballots'] += 1
    for r in read_csv_gz(paths['member_votes.csv.gz']):
        pid = wanted.get(r['member_id'])
        if not pid:
            continue
        position = POSITIONS.get(r['position'])
        if position is None:
            stats['absences'][pid] += 1
            continue
        upsert_vote(conn, f"ep-{r['vote_id']}", pid, position, r['group_short_label'] or None, None)
        stats['votes'][pid] += 1
    conn.execute('COMMIT')
    log(f"  PE ({release}): {stats['ballots']} ballots")
    return stats
