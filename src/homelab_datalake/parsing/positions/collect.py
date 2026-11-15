# Vendored from the politico repository (scripts/positions/collect.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""Collectors for the positions pipeline.

  python3 -I scripts/positions/collect.py people
  python3 -I scripts/positions/collect.py votes-an   --people pilot [--legislatures 15,16,17]
  python3 -I scripts/positions/collect.py votes-senat --people pilot
  python3 -I scripts/positions/collect.py votes-ep   --people pilot
  python3 -I scripts/positions/collect.py votes-ep-hix --people main   # EP 2004-2019 (VoteWatch / Hix)
  python3 -I scripts/positions/collect.py speeches   --people pilot [--limit N]
  python3 -I scripts/positions/collect.py debats-an  --people main
  python3 -I scripts/positions/collect.py debats-senat --people main
  python3 -I scripts/positions/collect.py programmes --people pilot
  python3 -I scripts/positions/collect.py archelec   --people historical   # presidential campaign documents 1965-2012
  python3 -I scripts/positions/collect.py all        --people main
  python3 -I scripts/positions/collect.py report     --people main

--people: pilot (Le Pen, Lisnard) | main (15 with polling weight) | candidates | all | id,id,...
Every download is cached under research/positions/raw/ with a .meta.json sidecar (URL, date,
sha256); re-running resumes and only fetches what is missing (--refresh to re-download).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))   # -I drops the script dir from sys.path

import argparse
import json

import people as people_mod
from common import Fetcher, log
from db import connect


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('command', choices=['people', 'votes-an', 'votes-senat', 'votes-ep', 'votes-ep-hix', 'speeches', 'debats-an', 'debats-senat', 'programmes', 'archelec', 'all', 'report'])
    ap.add_argument('--people', default='pilot')
    ap.add_argument('--legislatures', default='16,17', help='Assemblée legislatures (15, 16, 17 available)')
    ap.add_argument('--limit', type=int, help='speeches: fetch at most N texts per run')
    ap.add_argument('--revalidate', action='store_true', help='big cached downloads: HEAD-check them and fetch again only when the server copy changed')
    ap.add_argument('--refresh', action='store_true', help='re-download cached files')
    ap.add_argument('--per-person', type=int, help='speeches: at most N texts per person (most recent first)')
    ap.add_argument('--key-only', action='store_true', help='speeches: only major addresses (vœux, conférences de presse, déclarations…), no ceremonial items')
    ap.add_argument('--no-text', action='store_true', help='speeches: list documents without fetching texts')
    ap.add_argument('--json', action='store_true', help='report: print JSON')
    ap.add_argument('--db', help='database file (default research/positions/positions.sqlite, or $POSITIONS_DB)')
    args = ap.parse_args(argv)

    conn = connect(args.db) if args.db else connect()
    people_mod.sync(conn)
    if args.command == 'people':
        for r in conn.execute('SELECT id, name, kind, party, priority FROM people ORDER BY kind, priority, id'):
            print(f"{r['kind']:<10} {str(r['priority'] or ''):>3} {r['id']:<28} {r['name']:<28} {r['party'] or ''}")
        return
    ids = people_mod.resolve(conn, args.people)
    if args.command == 'report':
        import report
        report.main(conn, ids, as_json=args.json)
        return
    config = people_mod.load_config()
    fetcher = Fetcher(min_interval=1.0, per_host={'www.vie-publique.fr': 3.0}, revalidate=args.revalidate)
    steps = ['votes-an', 'votes-senat', 'votes-ep', 'votes-ep-hix', 'speeches', 'debats-an', 'debats-senat', 'programmes'] if args.command == 'all' else [args.command]
    results = {}
    for step in steps:
        log(f'== {step} ({len(ids)} people)')
        try:
            if step == 'votes-an':
                from collectors import an
                results[step] = an.run(conn, fetcher, ids, [int(x) for x in args.legislatures.split(',')], args.refresh, config)
            elif step == 'votes-senat':
                from collectors import senat
                results[step] = senat.run(conn, fetcher, ids, args.refresh, config)
            elif step == 'votes-ep':
                from collectors import ep
                results[step] = ep.run(conn, fetcher, ids, args.refresh, config=config)
            elif step == 'votes-ep-hix':
                from collectors import ep_hix
                results[step] = ep_hix.run(conn, fetcher, ids, args.refresh, config=config)
            elif step == 'speeches':
                from collectors import speeches
                results[step] = speeches.run(conn, fetcher, ids, args.refresh, args.limit, not args.no_text, args.per_person, args.key_only)
            elif step == 'debats-an':
                from collectors import an_debats
                results[step] = an_debats.run(conn, fetcher, ids, args.refresh)
            elif step == 'debats-senat':
                from collectors import senat_debats
                results[step] = senat_debats.run(conn, fetcher, ids, args.refresh)
            elif step == 'archelec':
                from collectors import archelec
                results[step] = archelec.run(conn, fetcher, ids, args.refresh)
            elif step == 'programmes':
                from collectors import programmes
                results[step] = programmes.run(conn, fetcher, ids, args.refresh)
        except Exception as e:  # one source failing must not stop the others; report it
            if conn.in_transaction:
                conn.execute('ROLLBACK')
            log(f'!! {step} failed: {type(e).__name__}: {e}')
            results[step] = {'error': f'{type(e).__name__}: {e}'}
    if fetcher.downloaded:
        results['_downloaded'] = fetcher.downloaded
    print(json.dumps(results, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
