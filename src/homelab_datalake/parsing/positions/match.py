# Vendored from the politico repository (scripts/positions/match.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Maps chamber members / speakers to our people ids.

A member matches a person when the normalised full names are identical (accents, case,
hyphens and apostrophes ignored). Never on surname alone: "Jean-Marie Le Pen" must not become
"Marine Le Pen". When we know the person's birth year and the source gives one, they must agree;
several members with the same name and no birth year to separate them is reported as ambiguous
and left unmatched. Manual pins in people.json `external_ids` win over everything.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from common import norm_name, now


@dataclass
class Member:
    external_id: str
    first: str
    last: str
    birth_year: int | None = None

    @property
    def full(self) -> str:
        return norm_name(f'{self.first} {self.last}')


@dataclass
class MatchReport:
    matched: dict[str, tuple[str, str]] = field(default_factory=dict)   # external_id -> (person, method)
    ambiguous: dict[str, list[str]] = field(default_factory=dict)       # person -> candidate external ids
    rejected: list[tuple[str, str, str]] = field(default_factory=list)  # (person, external_id, reason)


def person_keys(person: dict) -> set[str]:
    keys = {norm_name(person['name'])}
    for alias in person.get('aliases', []):
        keys.add(norm_name(alias))
    return keys


def match_members(people: list[dict], members: list[Member], pins: dict[str, str] | None = None) -> MatchReport:
    """people: dicts with id, name, birth_year (optional aliases). pins: external_id -> person id."""
    report = MatchReport()
    pins = pins or {}
    for ext, pid in pins.items():
        report.matched[ext] = (pid, 'override')
    pinned_people = set(pins.values())
    by_name: dict[str, list[Member]] = {}
    for m in members:
        by_name.setdefault(m.full, []).append(m)
    for p in people:
        if p['id'] in pinned_people:
            continue
        cands = [m for k in person_keys(p) for m in by_name.get(k, []) if m.external_id not in pins]
        if not cands:
            continue
        by = p.get('birth_year')
        if by:
            agree = [m for m in cands if m.birth_year == by]
            unknown = [m for m in cands if m.birth_year is None]
            for m in cands:
                if m.birth_year is not None and m.birth_year != by:
                    report.rejected.append((p['id'], m.external_id, f'birth year {m.birth_year} ≠ {by}'))
            if len(agree) == 1:
                report.matched[agree[0].external_id] = (p['id'], 'exact-name+birth-year')
                continue
            cands = agree or unknown
        if len(cands) == 1:
            report.matched[cands[0].external_id] = (p['id'], 'exact-name')
        elif len(cands) > 1:
            report.ambiguous[p['id']] = [m.external_id for m in cands]
    return report


def store(conn, source: str, report: MatchReport, members: list[Member]) -> None:
    names = {m.external_id: f'{m.first} {m.last}' for m in members}
    conn.execute('BEGIN')
    conn.execute('DELETE FROM person_external_ids WHERE source = ?', (source,))
    for ext, (pid, method) in report.matched.items():
        conn.execute('INSERT INTO person_external_ids (person, source, external_id, external_name, method, matched_at) VALUES (?,?,?,?,?,?)',
                     (pid, source, ext, names.get(ext), method, now()))
    conn.execute('COMMIT')


def load_people(conn) -> list[dict]:
    return [dict(r) for r in conn.execute('SELECT id, name, birth_year FROM people')]


def pins_for(config: dict, source: str) -> dict[str, str]:
    """people.json external_ids: {"marine-le-pen": {"an": "PA1234"}} → {"PA1234": "marine-le-pen"}."""
    out = {}
    for pid, ids in config.get('external_ids', {}).items():
        for ext in ([ids[source]] if isinstance(ids.get(source), str) else ids.get(source, [])):
            out[ext] = pid
    return out
