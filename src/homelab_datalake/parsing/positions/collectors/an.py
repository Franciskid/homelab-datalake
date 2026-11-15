# Vendored from the politico repository (scripts/positions/collectors/an.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Assemblée nationale roll calls (data.assemblee-nationale.fr).

Downloads, per legislature, the Scrutins.json.zip bulk file, plus the AMO30 file of every
actor since the XIth legislature (from the latest legislature's repository). Every ballot is
stored once; the votes of the selected people are stored per ballot.
"""
from __future__ import annotations

import json
import zipfile

from common import LICENCES, RAW, log, record_raw_file
from match import Member, load_people, match_members, pins_for, store

BASE = 'https://data.assemblee-nationale.fr/static/openData/repository'
SCRUTINS = {
    14: f'{BASE}/14/loi/scrutins/Scrutins_XIV.json.zip',
    15: f'{BASE}/15/loi/scrutins/Scrutins_XV.json.zip',
    16: f'{BASE}/16/loi/scrutins/Scrutins.json.zip',
    17: f'{BASE}/17/loi/scrutins/Scrutins.json.zip',
}
CURRENT = 17
ACTEURS = f'{BASE}/{CURRENT}/amo/tous_acteurs_mandats_organes_xi_legislature/AMO30_tous_acteurs_tous_mandats_tous_organes_historique.json.zip'
# XIV names the blocks 'pour'/'contre'; XV–XVII use 'pours'/'contres'.
POSITIONS = {'pours': 'pour', 'pour': 'pour', 'contres': 'contre', 'contre': 'contre', 'abstentions': 'abstention', 'nonVotants': 'non-votant'}


def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def text(x):
    if isinstance(x, dict):
        return None if x.get('@xsi:nil') == 'true' else x.get('#text')
    return x


def parse_acteurs(path) -> tuple[list[Member], dict[str, str]]:
    members, groups = [], {}
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if name.startswith('json/acteur/'):
                a = json.loads(z.read(name))['acteur']
                ident = a['etatCivil']['ident']
                born = text(a['etatCivil'].get('infoNaissance', {}).get('dateNais'))
                members.append(Member(text(a['uid']), ident['prenom'], ident['nom'], int(born[:4]) if born else None))
            elif name.startswith('json/organe/'):
                o = json.loads(z.read(name))['organe']
                if o.get('codeType') == 'GP':
                    groups[text(o['uid'])] = o.get('libelleAbrev') or o.get('libelle')
    return members, groups


def parse_scrutin(doc: dict) -> tuple[dict, list[dict]]:
    """One Scrutins JSON → (ballot row, [{acteur, position, group, by_proxy}])."""
    s = doc['scrutin']
    leg, num = s['legislature'], s['numero']
    synth = (s.get('syntheseVote') or {}).get('decompte') or {}
    objet = (s.get('objet') or {}).get('libelle')
    ballot = {
        'id': f'an-{leg}-{num}', 'chamber': 'AN', 'term': leg, 'number': num, 'date': s['dateScrutin'],
        'title': (s.get('titre') or objet or '').strip(), 'object': objet,
        'vote_type': (s.get('typeVote') or {}).get('libelleTypeVote'),
        'result': (s.get('sort') or {}).get('code'),
        'n_for': int(synth['pour']) if synth.get('pour') else None,
        'n_against': int(synth['contre']) if synth.get('contre') else None,
        'n_abstain': int(synth['abstentions']) if synth.get('abstentions') else None,
        'url': f'https://www.assemblee-nationale.fr/dyn/{leg}/scrutins/{num}',
        'extra': json.dumps({'uid': s['uid'], 'demandeur': (s.get('demandeur') or {}).get('texte'),
                             'mise_au_point': bool(s.get('miseAuPoint'))}, ensure_ascii=False),
    }
    votes = []
    groupes = (((s.get('ventilationVotes') or {}).get('organe') or {}).get('groupes') or {}).get('groupe')
    for g in as_list(groupes):
        nominatif = (g.get('vote') or {}).get('decompteNominatif')
        nominatif = nominatif if isinstance(nominatif, dict) else {}
        for key, position in POSITIONS.items():
            block = nominatif.get(key)          # "" (a string) when nobody voted that way, in some legislatures
            for v in as_list(block.get('votant') if isinstance(block, dict) else None):
                votes.append({'acteur': v['acteurRef'], 'position': position, 'group': g.get('organeRef'),
                              'by_proxy': {'true': 1, 'false': 0}.get(v.get('parDelegation'))})
    return ballot, votes


def run(conn, fetcher, person_ids: list[str], legislatures=(16, 17), refresh: bool = False, config: dict | None = None) -> dict:
    meta = fetcher.download(ACTEURS, RAW / 'an' / 'AMO30_acteurs.json.zip', refresh=refresh, stream=True)
    record_raw_file(conn, meta, LICENCES['an'])
    members, groups = parse_acteurs(RAW / 'an' / 'AMO30_acteurs.json.zip')
    report = match_members(load_people(conn), members, pins_for(config or {}, 'an'))
    store(conn, 'an', report, members)
    for pid, ids in report.ambiguous.items():
        log(f'  AN: ambiguous {pid}: {ids}')
    wanted = {ext: pid for ext, (pid, _) in report.matched.items() if pid in person_ids}
    stats = {'members': len(members), 'matched': {pid: ext for ext, (pid, _) in report.matched.items() if pid in person_ids},
             'ballots': 0, 'votes': {pid: 0 for pid in person_ids}}
    for leg in legislatures:
        url = SCRUTINS[leg]
        dest = RAW / 'an' / f'{leg}' / url.rsplit('/', 1)[1]
        meta = fetcher.download(url, dest, refresh=refresh, stream=True)
        raw_id = record_raw_file(conn, meta, LICENCES['an'])
        n = 0
        conn.execute('BEGIN')
        with zipfile.ZipFile(dest) as z:
            for name in z.namelist():
                if not name.endswith('.json'):
                    continue
                data = json.loads(z.read(name))
                # XIV is one file {"scrutins": {"scrutin": [...]}}; XV–XVII are one file per ballot.
                items = [{'scrutin': s} for s in as_list(data['scrutins']['scrutin'])] if 'scrutins' in data else [data]
                for item in items:
                    ballot, votes = parse_scrutin(item)
                    upsert_ballot(conn, ballot, raw_id)
                    n += 1
                    for v in votes:
                        pid = wanted.get(v['acteur'])
                        if pid:
                            upsert_vote(conn, ballot['id'], pid, v['position'], groups.get(v['group'], v['group']), v['by_proxy'])
                            stats['votes'][pid] += 1
        conn.execute('COMMIT')
        stats['ballots'] += n
        log(f'  AN legislature {leg}: {n} ballots')
    return stats


def upsert_ballot(conn, b: dict, raw_id: int | None) -> None:
    conn.execute(
        """INSERT INTO ballots (id, chamber, term, number, date, title, object, vote_type, result, n_for, n_against, n_abstain, url, raw_file, extra)
           VALUES (:id, :chamber, :term, :number, :date, :title, :object, :vote_type, :result, :n_for, :n_against, :n_abstain, :url, :raw_file, :extra)
           ON CONFLICT(id) DO UPDATE SET date=excluded.date, title=excluded.title, object=excluded.object,
             vote_type=excluded.vote_type, result=excluded.result, n_for=excluded.n_for, n_against=excluded.n_against,
             n_abstain=excluded.n_abstain, url=excluded.url, raw_file=excluded.raw_file, extra=excluded.extra""",
        {**b, 'raw_file': raw_id})


def upsert_vote(conn, ballot: str, person: str, position: str, group: str | None, by_proxy: int | None) -> None:
    conn.execute(
        """INSERT INTO votes (ballot, person, position, group_ref, by_proxy) VALUES (?,?,?,?,?)
           ON CONFLICT(ballot, person) DO UPDATE SET position=excluded.position, group_ref=excluded.group_ref, by_proxy=excluded.by_proxy""",
        (ballot, person, position, group, by_proxy))
