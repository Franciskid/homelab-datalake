# Vendored from the politico repository (scripts/positions/candidacies.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""New candidacies and withdrawals, proposed automatically and decided by a person.

  python3 -I scripts/positions/candidacies.py detect --out DIR [--polls polls-export.json,polls-index.json] [--watch programme-watch.json]
  python3 -I scripts/positions/candidacies.py review [--file candidacy-proposals.json]
  python3 -I scripts/positions/candidacies.py --accept ID [--file ...]     # writes people.json, then re-run the sync
  python3 -I scripts/positions/candidacies.py --dismiss ID [--file ...]    # remembers the decision in people.json

`detect` (server, daily) reads three independent signals and writes `candidacy-proposals.json` into DIR:
  (a) polls      a candidate id in an ingested poll (polls-export.json rounds, polls-index.json) that the roster does not know;
  (b) watcher    a programme published for a tracked person who had none (programme-watch.json), listed for information;
  (c) wikipedia  the French Wikipedia page "Candidatures à l'élection présidentielle française de 2027": names under
                 « Candidats déclarés » the roster does not know (new candidacy), roster candidates now under
                 « Candidatures retirées » (withdrawal), and any change of those lists since the previous run.
Nothing is added to the tracked people automatically: a proposal waits until someone runs `--accept` (new candidacy: adds the person
to `declared` in people.json; withdrawal: tags the candidate `withdrawn`) or `--dismiss`. Proposals whose person is known
or dismissed in people.json are dropped on the next run. Adding the person to the tracked set still needs the usual review of the
change (git diff of people.json), then a re-vendoring of the collector into the datalake.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import argparse
import html as htmllib
import json
import os
import re
from datetime import date

import people as people_mod
from common import Fetcher, log, norm_name, now

WIKI = 'https://fr.wikipedia.org/wiki/Candidatures_%C3%A0_l%27%C3%A9lection_pr%C3%A9sidentielle_fran%C3%A7aise_de_2027'
PROPOSALS = 'candidacy-proposals.json'
STATE = 'candidacy-state.json'
NEW_DAYS = 14        # a programme counts as newly published for this many days after the watcher first saw it
KEEP_DAYS = 30       # an informational proposal stays listed this long


# -- readers -------------------------------------------------------------------------------------------------------------
def _section(page: str, sid: str) -> str:
    i = page.find(f'id="{sid}"')
    if i < 0:
        return ''
    j = page.find('<h2', i + 10)
    return page[i:j if j > 0 else len(page)]


def _text(fragment: str) -> str:
    fragment = re.sub(r'<sup.*?</sup>', '', fragment, flags=re.S)
    return re.sub(r'\|+', '|', htmllib.unescape(re.sub(r'<[^>]+>', '|', fragment)))


def parse_wikipedia(page: str) -> dict:
    """{'declared': [{'name', 'party'}], 'withdrawn': [{'name'}]} from the page HTML (Parsoid output)."""
    declared = []
    for row in re.findall(r'<tr[^>]*>(.*?)</tr>', _section(page, 'Candidats_déclarés'), re.S):
        m = re.match(r'\s*<th[^>]*>(.*?)</th>', row, re.S)
        if not m:
            continue
        parts = [p.strip() for p in _text(m.group(1)).split('|') if p.strip()]
        if not parts or parts[0].startswith('Candidat '):
            continue
        name = parts[1] if ',' in parts[0] and len(parts) > 1 else parts[0]
        age = next((i for i, p in enumerate(parts) if re.search(r'\bans?\b', p)), None)
        party = None
        if age is not None:
            rest = [p for p in parts[age + 1:] if p not in ('/', '(', ')', '!')]
            party = rest[0] if rest else None
        declared.append({'name': name.strip(), 'party': party})
    withdrawn = []
    for li in re.findall(r'<li[^>]*>(.*?)</li>', _section(page, 'Candidatures_retirées'), re.S):
        parts = [p.strip() for p in _text(li).split('|') if p.strip()]
        if parts:
            withdrawn.append({'name': parts[0]})
    return {'declared': declared, 'withdrawn': withdrawn}


def poll_candidates(docs: list[dict]) -> dict[str, str]:
    """{id: name} of every candidate named by the given poll documents (polls-export rounds[].candidates, polls-index candidates)."""
    out: dict[str, str] = {}
    for d in docs:
        for r in d.get('rounds', []) if isinstance(d.get('rounds'), list) else []:
            if isinstance(r, dict) and isinstance(r.get('candidates'), dict):
                out.update({k: v for k, v in r['candidates'].items() if isinstance(v, str)})
        c = d.get('candidates')
        if isinstance(c, list):
            out.update({k: out.get(k, k) for k in c if isinstance(k, str)})
        elif isinstance(c, dict):
            out.update({k: v for k, v in c.items() if isinstance(v, str)})
    return out


def watcher_published(state: dict) -> dict[str, dict]:
    """{person: {key, url}} for targets whose verdict is `published`."""
    out = {}
    for key, t in (state.get('targets') or {}).items():
        if t.get('verdict') == 'published':
            out[t.get('person') or key.split('-')[0]] = {'key': key, 'url': (t.get('history') or [{}])[-1].get('url'),
                                                         'first_published': t.get('first_published')}
    return out


# -- roster --------------------------------------------------------------------------------------------------------------
def roster(config: dict | None = None) -> dict:
    """Everything already decided: people tracked (any kind) by id and by normalised name, dismissed ids, ignored poll ids."""
    config = config or people_mod.load_config()
    people = people_mod.build_people(config=config)
    by_name = {norm_name(p['name']): p['id'] for p in people}
    declared = config.get('declared', {})
    for pid, m in declared.items():
        by_name.setdefault(norm_name(m['name']), pid)
    ids = {p['id'] for p in people} | set(declared)
    withdrawn = {pid for pid, m in config.get('candidates', {}).items() if 'withdrawn' in m.get('tags', [])}
    return {'ids': ids, 'by_name': by_name, 'dismissed': set(config.get('candidacy_dismissed', [])),
            'ignored': set(config.get('exclude', [])), 'withdrawn': withdrawn,
            'candidates': {p['id'] for p in people if p['kind'] == 'candidate'},
            'names': {p['id']: p['name'] for p in people}}


def _slug(name: str) -> str:
    return norm_name(name).replace(' ', '-')


def known_id(r: dict, pid: str | None, name: str | None) -> str | None:
    if pid and pid in r['ids']:
        return pid
    if name and norm_name(name) in r['by_name']:
        return r['by_name'][norm_name(name)]
    if name and _slug(name) in r['ids']:
        return _slug(name)
    return None


# -- detection -----------------------------------------------------------------------------------------------------------
def detect(r: dict, wiki: dict | None, polls: dict[str, str], watch: dict, previous: dict | None, state: dict, today: str) -> list[dict]:
    """The open proposals of this run (merged with the previous file by id: first_seen and earlier signals are kept)."""
    found: dict[str, dict] = {}

    def add(pid, kind, name, party, signal):
        key = f'{kind}:{pid}'
        p = found.setdefault(key, {'id': key, 'kind': kind, 'person': pid, 'name': name, 'party': party, 'signals': []})
        p['party'] = p['party'] or party
        p['signals'].append(signal)

    if wiki:
        for c in wiki['declared']:
            k = known_id(r, None, c['name'])
            if k is None:
                add(_slug(c['name']), 'new-candidacy', c['name'], c.get('party'),
                    {'source': 'wikipedia', 'detail': 'listed under « Candidats déclarés »', 'url': WIKI})
            elif k not in r['candidates'] and k in r['ids']:
                pass   # a tracked orbit / historical person who declared: a human decides (below)
        for c in wiki['withdrawn']:
            k = known_id(r, None, c['name'])
            if k in r['candidates'] and k not in r['withdrawn']:
                add(k, 'withdrawal', c['name'], None, {'source': 'wikipedia', 'detail': 'listed under « Candidatures retirées »', 'url': WIKI})
        old = (state.get('wikipedia') or {})
        if old:
            gone = {norm_name(x['name']) for x in old.get('declared', [])} - {norm_name(x['name']) for x in wiki['declared']}
            for x in old.get('declared', []):
                k = known_id(r, None, x['name'])
                if norm_name(x['name']) in gone and k in r['candidates'] and k not in r['withdrawn']:
                    add(k, 'withdrawal', x['name'], None, {'source': 'wikipedia', 'detail': 'no longer listed under « Candidats déclarés »', 'url': WIKI})
    for pid, name in polls.items():
        if pid in r['ignored']:
            continue
        if known_id(r, pid, name) is None:
            add(pid, 'new-candidacy', name, None, {'source': 'polls', 'detail': f'candidate id {pid!r} in an ingested poll'})
    for person, w in watch.items():
        fresh = w.get('first_published') and (date.fromisoformat(today) - date.fromisoformat(w['first_published'])).days <= NEW_DAYS
        if person in r['candidates'] and fresh:
            add(person, 'programme-published', r['names'].get(person, person), None,
                {'source': 'watcher', 'detail': f"programme published ({w['key']})", 'url': w.get('url')})
    prev = {p['id']: p for p in (previous or {}).get('proposals', [])}
    out = []
    for key, p in sorted(found.items()):
        if p['person'] in r['dismissed'] or (p['kind'] == 'new-candidacy' and known_id(r, p['person'], p['name'])):
            continue
        old = prev.get(key, {})
        seen = {(s['source'], s['detail']) for s in old.get('signals', [])}
        p['signals'] = old.get('signals', []) + [s for s in p['signals'] if (s['source'], s['detail']) not in seen]
        p['first_seen'] = old.get('first_seen') or today
        p['last_seen'] = today
        p['status'] = 'open'
        out.append(p)
    # a one-off signal (a programme published) is not seen again tomorrow: keep it listed for a while
    for key, old in prev.items():
        if key not in found and old['kind'] == 'programme-published' and old['person'] not in r['dismissed'] \
                and (date.fromisoformat(today) - date.fromisoformat(old['last_seen'])).days <= KEEP_DAYS:
            out.append(old)
    return out


def run_detect(out_dir: Path, fetcher, polls_files: list[Path], watch_file: Path | None, wikipedia: bool = True, today: str | None = None) -> dict:
    today = today or date.today().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / STATE
    state = json.loads(state_path.read_text()) if state_path.exists() else {}
    previous = json.loads((out_dir / PROPOSALS).read_text()) if (out_dir / PROPOSALS).exists() else None
    errors = []
    wiki = None
    if wikipedia:
        try:
            data, _ = fetcher.get_bytes(WIKI)
            wiki = parse_wikipedia(data.decode('utf-8', 'replace'))
            if not wiki['declared']:
                errors.append('wikipedia: the page was read but no declared candidate was found (layout changed?)')
                wiki = None
        except Exception as e:   # noqa: BLE001 -- one source failing must not stop the others
            errors.append(f'wikipedia: {type(e).__name__}: {e}')
    docs = []
    for f in polls_files:
        try:
            docs.append(json.loads(f.read_text()))
        except (OSError, ValueError) as e:
            errors.append(f'polls: {f.name}: {e}')
    watch_state = {}
    if watch_file:
        try:
            watch_state = json.loads(watch_file.read_text())
        except (OSError, ValueError) as e:
            errors.append(f'watcher: {e}')
    r = roster()
    proposals = detect(r, wiki, poll_candidates(docs), watcher_published(watch_state), previous, state, today)
    result = {'generated_at': now(), 'proposals': proposals, 'errors': errors,
              'sources': {'wikipedia': bool(wiki), 'polls_files': len(docs), 'watcher': bool(watch_state)}}
    if wiki:
        state['wikipedia'] = {**wiki, 'seen_at': today}
    for name, payload in ((STATE, state), (PROPOSALS, result)):
        tmp = out_dir / f'.{name}.tmp'
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        os.replace(tmp, out_dir / name)
    return result


# -- review (the Mac) ----------------------------------------------------------------------------------------------------
def open_proposals(doc: dict, config: dict | None = None) -> list[dict]:
    """The file's proposals that people.json has not already settled."""
    r = roster(config)
    out = []
    for p in doc.get('proposals', []):
        if p['person'] in r['dismissed']:
            continue
        if p['kind'] == 'new-candidacy' and known_id(r, p['person'], p['name']):
            continue
        if p['kind'] == 'withdrawal' and p['person'] in r['withdrawn']:
            continue
        out.append(p)
    return out


def accept(proposal: dict, config: dict, today: str) -> str:
    """Apply one proposal to the people.json document (in place); returns what was done."""
    pid = proposal['person']
    if proposal['kind'] == 'new-candidacy':
        config.setdefault('declared', {})[pid] = {
            'name': proposal['name'], 'party': proposal.get('party'), 'birth_year': None, 'tags': [],
            'accepted_on': today, 'signals': [s['source'] for s in proposal['signals']]}
        return f'added {pid} to declared (fill birth_year, it tells homonyms apart when matching chamber members)'
    if proposal['kind'] == 'withdrawal':
        meta = config.setdefault('candidates', {}).setdefault(pid, {})
        meta['tags'] = sorted(set(meta.get('tags', [])) | {'withdrawn'})
        meta['withdrawn_on'] = today
        return f'tagged {pid} as withdrawn'
    raise SystemExit(f"{proposal['kind']} proposals are informational: nothing to accept (use --dismiss to hide it)")


def dismiss(proposal: dict, config: dict) -> str:
    config.setdefault('candidacy_dismissed', [])
    if proposal['person'] not in config['candidacy_dismissed']:
        config['candidacy_dismissed'].append(proposal['person'])
    return f"dismissed {proposal['person']}"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('command', nargs='?', choices=['detect', 'review'], default='review')
    ap.add_argument('--out', help='detect: directory for candidacy-proposals.json')
    ap.add_argument('--polls', default='', help='detect: comma-separated poll JSON files')
    ap.add_argument('--watch', help='detect: programme-watch.json')
    ap.add_argument('--no-wikipedia', action='store_true')
    ap.add_argument('--file', help='review: the proposals file (default: research/positions/sync/candidacy-proposals.json)')
    ap.add_argument('--accept', metavar='ID')
    ap.add_argument('--dismiss', metavar='ID')
    args = ap.parse_args(argv)
    if args.command == 'detect' and not (args.accept or args.dismiss):
        res = run_detect(Path(args.out), Fetcher(min_interval=2.0), [Path(x) for x in args.polls.split(',') if x],
                         Path(args.watch) if args.watch else None, not args.no_wikipedia)
        print(json.dumps({'proposals': len(res['proposals']), 'errors': res['errors'], 'sources': res['sources']}, ensure_ascii=False, indent=1))
        return
    from common import RESEARCH
    path = Path(args.file) if args.file else RESEARCH / 'sync' / PROPOSALS
    doc = json.loads(path.read_text()) if path.exists() else {'proposals': []}
    config = people_mod.load_config()
    if args.accept or args.dismiss:
        want = args.accept or args.dismiss
        match = [p for p in doc['proposals'] if want in (p['id'], p['person'])]
        if len(match) != 1:
            raise SystemExit(f'{want!r} matches {len(match)} proposals: use the full id (kind:person) from `review`')
        msg = accept(match[0], config, date.today().isoformat()) if args.accept else dismiss(match[0], config)
        people_mod.CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=1) + '\n')
        print(msg)
        return
    rows = open_proposals(doc, config)
    print(f"{len(rows)} open proposal(s) (file generated {doc.get('generated_at', '?')}, errors: {doc.get('errors') or 'none'})")
    for p in rows:
        print(f"  {p['id']:<44} {p['name']:<26} {p.get('party') or '':<24} first seen {p['first_seen']}  via {', '.join(sorted({s['source'] for s in p['signals']}))}")


if __name__ == '__main__':
    main()
