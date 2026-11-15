# Vendored from the politico repository (scripts/positions/people.py, bf1694c). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Builds the people table from src/data/fr-polling-2027.json and scripts/positions/people.json.

Priority: the 15 candidates "with polling weight" come first. Rule (deterministic, recomputed
from the poll file): over the 12 months before the latest poll, keep candidates tested in at
least 10 scenarios and rank them by their mean score; the top 15 get priority 1..15, every
other candidate follows by the same mean (then by name).
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, timedelta

from common import HERE, ROOT

POLLS = ROOT / 'src' / 'data' / 'fr-polling-2027.json'
CONFIG = HERE / 'people.json'
# Where the poll file is not available (the server's vendored copy), the roster written by `write_roster` stands in for it.
ROSTER = HERE / 'people-roster.json'
MAIN_COUNT = 15
MIN_SCENARIOS = 10
WINDOW_DAYS = 365


def load_config() -> dict:
    return json.loads(CONFIG.read_text())


def polling_rank(polls: dict, exclude: set[str]) -> list[tuple[str, float, int]]:
    """[(id, mean score, scenarios)] for first-round polls in the window, best first."""
    rounds = polls['rounds']
    first = rounds[0]
    latest = max(date.fromisoformat(p['end']) for p in first['polls'])
    start = latest - timedelta(days=WINDOW_DAYS)
    scores = defaultdict(list)
    for p in first['polls']:
        if date.fromisoformat(p['end']) < start:
            continue
        for s in p['scenarios']:
            for k, v in s['scores'].items():
                if k not in exclude:
                    scores[k].append(v or 0)
    rows = [(k, sum(v) / len(v), len(v)) for k, v in scores.items()]
    return sorted(rows, key=lambda r: (-(r[2] >= MIN_SCENARIOS), -r[1], r[0]))


def write_roster(path=ROSTER) -> int:
    """Freeze build_people() (needs the poll file) into a JSON roster that works without it."""
    people = build_people(json.loads(POLLS.read_text()))
    path.write_text(json.dumps(people, ensure_ascii=False, indent=1) + '\n')
    return len(people)


def build_people(polls: dict | None = None, config: dict | None = None) -> list[dict]:
    if polls is None and not POLLS.exists() and ROSTER.exists():
        return json.loads(ROSTER.read_text())
    polls = polls or json.loads(POLLS.read_text())
    config = config or load_config()
    exclude = set(config.get('exclude', []))
    names = {k: v for k, v in polls['rounds'][0]['candidates'].items() if k not in exclude}
    ranked = [r[0] for r in polling_rank(polls, exclude) if r[0] in names]
    ranked += sorted(k for k in names if k not in ranked)
    historical = config.get('historical', {})
    people = []
    for i, pid in enumerate(ranked, 1):
        if pid in historical:
            continue
        meta = config['candidates'].get(pid, {})
        people.append({'id': pid, 'name': names[pid], 'kind': 'candidate', 'party': meta.get('party'),
                       'orbit_of': None, 'orbit_weight': None, 'priority': i,
                       'birth_year': meta.get('birth_year'), 'tags': meta.get('tags', [])})
    # Renumber candidates contiguously after removing historical ids.
    for i, p in enumerate(people, 1):
        p['priority'] = i
    # Candidacies accepted by a person (candidacies.py --accept) that the bundled polls do not name yet: tracked after the polled ones.
    for pid, meta in config.get('declared', {}).items():
        if pid not in names and pid not in historical:
            people.append({'id': pid, 'name': meta['name'], 'kind': 'candidate', 'party': meta.get('party'),
                           'orbit_of': None, 'orbit_weight': None, 'priority': len(people) + 1,
                           'birth_year': meta.get('birth_year'), 'tags': meta.get('tags', [])})
    for i, p in enumerate(people, 1):
        p['priority'] = i
    for pid, meta in historical.items():
        people.append({'id': pid, 'name': meta.get('name') or names.get(pid, pid), 'kind': 'historical',
                       'party': meta.get('party'), 'orbit_of': None, 'orbit_weight': None, 'priority': None,
                       'birth_year': meta.get('birth_year'), 'tags': meta.get('tags', [])})
    for pid, meta in config.get('orbit', {}).items():
        people.append({'id': pid, 'name': meta['name'], 'kind': 'orbit', 'party': meta.get('party'),
                       'orbit_of': meta['orbit_of'], 'orbit_weight': meta['orbit_weight'], 'priority': None,
                       'birth_year': meta.get('birth_year'), 'tags': meta.get('tags', [])})
    return people


def sync(conn, people: list[dict] | None = None) -> int:
    people = people if people is not None else build_people()
    conn.execute('BEGIN')
    for p in sorted(people, key=lambda p: p['kind'] == 'orbit'):   # anchors before their orbit
        conn.execute(
            """INSERT INTO people (id, name, kind, party, orbit_of, orbit_weight, priority, birth_year, tags)
               VALUES (:id, :name, :kind, :party, :orbit_of, :orbit_weight, :priority, :birth_year, :tags)
               ON CONFLICT(id) DO UPDATE SET name=excluded.name, kind=excluded.kind, party=excluded.party,
                 orbit_of=excluded.orbit_of, orbit_weight=excluded.orbit_weight, priority=excluded.priority,
                 birth_year=excluded.birth_year, tags=excluded.tags""",
            {**p, 'tags': json.dumps(p['tags'])})
    conn.execute('COMMIT')
    return len(people)


def main_ids(conn, n: int = MAIN_COUNT) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT id FROM people WHERE kind = 'candidate' AND priority <= ? ORDER BY priority", (n,))]


def resolve(conn, spec: str) -> list[str]:
    """'pilot' | 'main' | 'candidates' | 'orbit' | 'tracked' | 'historical' | comma-separated ids."""
    if spec == 'pilot':
        return ['marine-le-pen', 'david-lisnard']
    if spec == 'main':
        return main_ids(conn)
    if spec == 'candidates':
        return [r[0] for r in conn.execute("SELECT id FROM people WHERE kind = 'candidate' ORDER BY priority")]
    if spec == 'tracked':   # what the automatic collection follows: the candidates and their orbit
        return [r[0] for r in conn.execute("SELECT id FROM people WHERE kind IN ('candidate', 'orbit') ORDER BY kind, priority, orbit_of, id")]
    if spec in ('orbit', 'historical'):
        return [r[0] for r in conn.execute('SELECT id FROM people WHERE kind = ? ORDER BY orbit_of, id', (spec,))]
    if spec == 'all':
        return [r[0] for r in conn.execute('SELECT id FROM people ORDER BY kind, priority, id')]
    ids = [s.strip() for s in spec.split(',') if s.strip()]
    known = {r[0] for r in conn.execute('SELECT id FROM people')}
    unknown = [i for i in ids if i not in known]
    if unknown:
        raise SystemExit(f'unknown people: {", ".join(unknown)}')
    return ids
