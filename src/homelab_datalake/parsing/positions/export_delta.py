# Vendored from the politico repository (scripts/positions/export_delta.py, bf1694c). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Server side: what is new in the collection database since the last export, as a small delta the Mac can pull.

  python3 -I scripts/positions/export_delta.py --db positions.sqlite --out /exports/positions [--runs runs.json] [--keep-days 60]

Writes into --out (the directory the Mac rsyncs):
  manifest.json              latest state: sequence number, deltas still kept, the new documents of the latest delta, counts
  manifests/manifest-<seq>.json   the manifest of each delta (new documents, provenance), kept as long as the delta
  deltas/delta-<seq>-<UTC>.sqlite.gz   a database with the same schema holding only what is new: people (all, small), the fetched
                                       documents whose text changed, their speaker turns, ballots and votes not exported yet,
                                       and the provenance rows (raw_files) they point to
  files/<relative path>      the text file of every exported document (same relative paths as on the Mac, so merge-db copies them)
  .export-state.json         what was already exported (document key → sha256, last vote rowid). Never read by the Mac.
Bulk dumps (the raw zips of the open-data portals) are never exported: only text files and provenance rows.
Nothing new → no delta, the manifest only says so. Idempotent: the Mac may apply a delta twice (merge-db upserts).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # -I drops the script dir from sys.path

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

from common import ROOT, now, sha256_file
from db import connect, version

STATE = '.export-state.json'
MANIFEST_VERSION = 1
INLINE_DOCS = 300   # the top-level manifest lists the new documents inline up to this many


def _rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args)]


def _insert(conn, table, rows):
    if not rows:
        return
    cols = list(rows[0])
    conn.executemany(f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)})", rows)


def read_state(out: Path) -> dict:
    try:
        return json.loads((out / STATE).read_text())
    except (OSError, ValueError):
        return {'seq': 0, 'docs': {}, 'vote_mark': 0, 'deltas': []}


def doc_key(d) -> str:
    return f"{d['source']}|{d['external_id']}|{d['person']}"


def build_delta(src: sqlite3.Connection, state: dict, root: Path, tmp_db: Path, since: str | None = None) -> dict | None:
    """Fill tmp_db (fresh, migrated) with what is new; None when nothing is."""
    docs = [d for d in _rows(src, "SELECT * FROM documents WHERE status = 'fetched' ORDER BY id")
            if state['docs'].get(doc_key(d)) != d['sha256'] and (not since or (d['date'] or '9999') >= since)]
    vote_mark = state['vote_mark']
    votes = _rows(src, 'SELECT rowid AS _rowid, * FROM votes WHERE rowid > ? ORDER BY rowid', (vote_mark,))
    if since:   # a window (dry run, reporting): only the ballots dated inside it
        recent = {r[0] for r in src.execute('SELECT id FROM ballots WHERE date >= ?', (since,))}
        votes = [v for v in votes if v['ballot'] in recent]
    if not docs and not votes:
        return None
    d = connect(tmp_db)
    try:
        d.execute('BEGIN')
        _insert(d, 'people', _rows(src, 'SELECT * FROM people ORDER BY orbit_of IS NOT NULL, id'))
        _insert(d, 'person_external_ids', _rows(src, 'SELECT * FROM person_external_ids'))
        raw_ids = {x['raw_file'] for x in docs if x['raw_file']}
        ballot_ids = sorted({v['ballot'] for v in votes})
        ballots = [b for i in range(0, len(ballot_ids), 500)
                   for b in _rows(src, f"SELECT * FROM ballots WHERE id IN ({','.join('?' * len(ballot_ids[i:i + 500]))})", ballot_ids[i:i + 500])]
        raw_ids |= {b['raw_file'] for b in ballots if b['raw_file']}
        raws = [r for i in sorted(raw_ids) for r in _rows(src, 'SELECT * FROM raw_files WHERE id = ?', (i,))]
        _insert(d, 'raw_files', raws)
        _insert(d, 'ballots', ballots)
        _insert(d, 'votes', [{k: v for k, v in x.items() if k != '_rowid'} for x in votes])
        _insert(d, 'documents', docs)
        old_ids = [x['id'] for x in docs]
        turns = [t for i in range(0, len(old_ids), 500)
                 for t in _rows(src, f"SELECT * FROM document_turns WHERE document IN ({','.join('?' * len(old_ids[i:i + 500]))})", old_ids[i:i + 500])]
        _insert(d, 'document_turns', turns)
        _insert(d, 'coverage_notes', _rows(src, 'SELECT * FROM coverage_notes'))
        d.execute('COMMIT')
    finally:
        d.close()
    return {'documents': docs, 'votes': len(votes), 'ballots': len(ballots), 'turns': len(turns), 'raw_files': len(raws),
            'vote_mark': max((v['_rowid'] for v in votes), default=vote_mark)}


def new_doc_entry(d: dict) -> dict:
    return {'person': d['person'], 'source': d['source'], 'source_type': d['source_type'], 'external_id': d['external_id'],
            'url': d['url'], 'title': d['title'], 'date': d['date'], 'sha256': d['sha256'], 'chars': d['chars'],
            'path': d['path'], 'licence': d['licence'], 'attribution': d['attribution'], 'fetched_at': d['fetched_at']}


def export(db_path, out: Path, root: Path = ROOT, runs: list | None = None, keep_days: int = 60, now_ts: str | None = None,
           since: str | None = None) -> dict:
    out = Path(out)
    for sub in ('deltas', 'manifests', 'files'):
        (out / sub).mkdir(parents=True, exist_ok=True)
    state = read_state(out)
    started = now_ts or now()
    src = connect(db_path)
    try:
        with tempfile.TemporaryDirectory(dir=out) as t:
            tmp_db = Path(t) / 'delta.sqlite'
            built = build_delta(src, state, root, tmp_db, since)
            entry = None
            if built:
                seq = state['seq'] + 1
                stamp = started.replace('-', '').replace(':', '').replace('+0000', 'Z').replace('+00:00', 'Z')
                name = f'delta-{seq:06d}-{stamp}.sqlite.gz'
                missing = []
                for d in built['documents']:
                    if d['path']:
                        src_file = root / d['path']
                        dst_file = out / 'files' / d['path']
                        if not src_file.exists():
                            missing.append(d['path'])
                        elif not dst_file.exists() or sha256_file(dst_file) != d['sha256']:
                            dst_file.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(src_file, dst_file)
                gz_tmp = out / 'deltas' / (name + '.part')
                with open(tmp_db, 'rb') as f, gzip.open(gz_tmp, 'wb', compresslevel=6) as g:
                    shutil.copyfileobj(f, g)
                os.replace(gz_tmp, out / 'deltas' / name)
                new_docs = [new_doc_entry(d) for d in built['documents']]
                entry = {'seq': seq, 'file': f'deltas/{name}', 'sha256': sha256_file(out / 'deltas' / name),
                         'bytes': (out / 'deltas' / name).stat().st_size, 'created_at': started, 'schema_version': version(src),
                         'counts': {'documents': len(new_docs), 'ballots': built['ballots'], 'votes': built['votes'],
                                    'turns': built['turns'], 'raw_files': built['raw_files']},
                         'files_missing': missing}
                per_person: dict = {}
                for e in new_docs:
                    per_person.setdefault(e['person'], {}).setdefault(e['source'], 0)
                    per_person[e['person']][e['source']] += 1
                sub = {'version': MANIFEST_VERSION, 'delta': entry, 'since': state.get('last_export_at'), 'new_documents': new_docs,
                       'new_documents_per_person': per_person, 'runs': runs or []}
                (out / 'manifests' / f'manifest-{seq:06d}.json').write_text(json.dumps(sub, ensure_ascii=False, indent=1))
                state['seq'] = seq
                state['vote_mark'] = built['vote_mark']
                for d in built['documents']:
                    state['docs'][doc_key(d)] = d['sha256']
                state['deltas'].append({k: entry[k] for k in ('seq', 'file', 'sha256', 'bytes', 'created_at', 'schema_version', 'counts')})
    finally:
        src.close()
    # retention: deltas and their manifests older than keep_days leave the manifest and the disk (the files/ mirror stays)
    cutoff = (datetime.fromisoformat(started) - timedelta(days=keep_days)).isoformat()
    kept = []
    for e in state['deltas']:
        if e['created_at'] < cutoff:
            (out / e['file']).unlink(missing_ok=True)
            (out / 'manifests' / f"manifest-{e['seq']:06d}.json").unlink(missing_ok=True)
        else:
            kept.append(e)
    state['deltas'] = kept
    state['last_export_at'] = started
    latest_path = out / 'manifests' / f"manifest-{state['seq']:06d}.json"
    latest = None
    if state['seq'] and latest_path.exists():
        latest = json.loads(latest_path.read_text())
        latest['manifest'] = f'manifests/{latest_path.name}'
        if len(latest['new_documents']) > INLINE_DOCS:   # a backfill: the list stays in its own manifest file
            latest['new_documents_count'] = len(latest.pop('new_documents'))
    manifest = {'version': MANIFEST_VERSION, 'generated_at': started, 'seq': state['seq'], 'new_in_this_export': entry is not None,
                'deltas': kept, 'latest': latest, 'runs': runs or []}
    for name, payload in ((STATE, state), ('manifest.json', manifest)):
        tmp = out / f'.{name}.tmp'
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1))
        os.replace(tmp, out / name)
    return manifest


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--db', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--runs', help='JSON file describing the collection runs of this cycle (copied into the manifest)')
    ap.add_argument('--keep-days', type=int, default=60)
    ap.add_argument('--since', help='only documents and ballots dated on or after this day (a window: dry runs, reports)')
    args = ap.parse_args(argv)
    runs = json.loads(Path(args.runs).read_text()) if args.runs else None
    m = export(args.db, Path(args.out), runs=runs, keep_days=args.keep_days, since=args.since)
    d = m['latest']['delta'] if m['new_in_this_export'] and m['latest'] else None
    print(json.dumps({'seq': m['seq'], 'new': m['new_in_this_export'], 'delta': d}, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
