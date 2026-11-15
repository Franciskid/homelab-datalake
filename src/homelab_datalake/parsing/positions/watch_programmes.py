# Vendored from the politico repository (scripts/positions/watch_programmes.py, e83b8f0). Do not edit here: change it there and run scripts/vendor-positions.py.
"""2027 programme watcher.

  python3 -I scripts/positions/watch_programmes.py [--people main|id,id] [--db FILE] [--no-db] [--refresh]

For each target in `watch-targets.json` (an official site or programme page of a main candidate, each with where the URL was
seen), fetches the page (robots.txt respected, 1 request per target plus at most 3 linked programme pages), and records in
`research/positions/programme-watch.json` (tracked): HTTP result, text hash and length, first-seen / last-changed dates, the
programme-looking links found, and a verdict:
  - `published`   a programme document was found (a PDF of 4+ pages worth of text, or an HTML page of 6,000+ characters under a
                  programme/projet address) → its text is stored as a `programme` document (source `programme-watch`, id =
                  key + hash) and a new version is stored each time it changes materially;
  - `not-found`   page reachable, nothing that looks like a full programme;
  - `unreachable` network error, 4xx/5xx or robots.txt refusal (the reason is kept; nothing is hidden).
A change is `material` when fewer than 97 % of the lines are shared with the previous version (menus, dates and counters move
all the time). The watcher never invents a programme: `published` needs the length test above, nothing else.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import argparse
import hashlib
import json
import re
import urllib.parse

from common import RAW, RESEARCH, CircuitOpen, FetchError, Fetcher, log, now, rel, sha256_file, record_raw_file
from collectors.programmes import html_to_text, pdf_to_text, clean_pdf_text

HERE = Path(__file__).resolve().parent
TARGETS = HERE / 'watch-targets.json'
STATE = RESEARCH / 'programme-watch.json'
FOLDER = RAW / 'watch'
MIN_PROGRAMME_CHARS = 6000
MATERIAL = 0.97            # share of identical lines below which a change counts
PROGRAMME_WORDS = re.compile(r'programme|projet|propositions|engagements|livre|pacte|manifeste|plan\b', re.I)
LICENCE = 'Document publié par son auteur (candidat ou parti) ; droits réservés, usage de citation'


def normalise(text: str) -> str:
    return re.sub(r'[ \t ]+', ' ', text).strip()


def text_hash(text: str) -> str:
    return hashlib.sha256(re.sub(r'\s+', ' ', text).strip().encode()).hexdigest()


def shared_lines(a: str, b: str) -> float:
    """Share of lines two versions have in common (Jaccard on non-empty lines)."""
    sa = {l.strip() for l in a.split('\n') if l.strip()}
    sb = {l.strip() for l in b.split('\n') if l.strip()}
    return len(sa & sb) / len(sa | sb) if sa | sb else 1.0


def programme_links(html: str, base: str, limit: int = 8) -> list[str]:
    """Links whose address or visible label says programme / projet / propositions (PDF first, then same-site pages)."""
    out = []
    for m in re.finditer(r'<a\b[^>]*href="([^"#]+)"[^>]*>(.*?)</a>', html, re.S | re.I):
        href, label = m.group(1), re.sub(r'<[^>]+>|\s+', ' ', m.group(2)).strip()
        if href.startswith(('mailto:', 'tel:', 'javascript:')):
            continue
        url = urllib.parse.urljoin(base, href.replace('&amp;', '&'))
        if urllib.parse.urlsplit(url).scheme not in ('http', 'https'):
            continue
        if PROGRAMME_WORDS.search(urllib.parse.urlsplit(url).path) or PROGRAMME_WORDS.search(label[:80]):
            if url not in out:
                out.append(url)
    out.sort(key=lambda u: (not u.lower().split('?')[0].endswith('.pdf'), urllib.parse.urlsplit(u).netloc != urllib.parse.urlsplit(base).netloc))
    return out[:limit]


def classify(url: str, data: bytes, ctype: str | None) -> tuple[str, str]:
    """(kind, text) for a fetched body: 'pdf' | 'html' | 'other'."""
    if data[:5] == b'%PDF-':
        return 'pdf', ''
    if 'html' in (ctype or '').lower() or b'<html' in data[:2000].lower() or b'<!doctype' in data[:200].lower():
        return 'html', html_to_text(data.decode('utf-8', 'replace'))
    return 'other', ''


def fetch_page(fetcher, url: str, folder: Path, key: str):
    """→ dict(kind, text, html, meta). Raises FetchError."""
    if not fetcher.allowed(url):
        raise FetchError(f'robots.txt disallows {url}')
    data, info = fetcher.get_bytes(url)
    kind, text = classify(url, data, info.get('content_type'))
    folder.mkdir(parents=True, exist_ok=True)
    html = None
    if kind == 'pdf':
        path = folder / f'{key}.pdf'
        path.write_bytes(data)
        text = clean_pdf_text(pdf_to_text(path))
    elif kind == 'html':
        html = data.decode('utf-8', 'replace')
    return {'kind': kind, 'text': text, 'html': html, 'info': info, 'bytes': len(data)}


def verdict(page: dict, url: str) -> bool:
    """Is this fetched document itself a full programme?"""
    n = len(page['text'])
    if page['kind'] == 'pdf':
        return n >= MIN_PROGRAMME_CHARS
    return page['kind'] == 'html' and n >= MIN_PROGRAMME_CHARS and bool(PROGRAMME_WORDS.search(urllib.parse.urlsplit(url).path))


def check_target(fetcher, t: dict, prev: dict | None, today: str) -> tuple[dict, dict | None]:
    """→ (new state entry, the programme page found or None). Never raises on network errors."""
    state = {**(prev or {}), 'last_checked': today, 'url': t['url']}
    folder = FOLDER / t['key']
    best = None
    try:
        page = fetch_page(fetcher, t['url'], folder, 'target')
        state.update(status='ok', error=None, content_type=page['info'].get('content_type'))
        if verdict(page, t['url']):
            best = (t['url'], page)
        links = programme_links(page['html'], page['info']['final_url']) if page['html'] else []
        state['links'] = links
        if best is None:
            for link in links[:3]:
                try:
                    sub = fetch_page(fetcher, link, folder, 'linked')
                except CircuitOpen:
                    raise
                except FetchError as e:
                    state.setdefault('link_errors', {})[link] = str(e)[:120]
                    continue
                if verdict(sub, link):
                    best = (link, sub)
                    break
        state['target_chars'] = len(page['text'])
        state['target_hash'] = text_hash(page['text'])
    except FetchError as e:
        state.update(status='unreachable', error=str(e)[:200])
    except Exception as e:      # pdftotext missing, odd encodings: reported, not fatal
        state.update(status='unreachable', error=f'{type(e).__name__}: {e}'[:200])

    history = state.setdefault('history', [])
    if best is None:
        state['verdict'] = 'unreachable' if state.get('status') == 'unreachable' else 'not-found'
        if not history or history[-1]['verdict'] != state['verdict']:
            history.append({'date': today, 'verdict': state['verdict'], 'note': state.get('error')})
        return state, None
    url, page = best
    h = text_hash(page['text'])
    old_text = None
    if state.get('programme_text_path') and Path(state['programme_text_path']).exists():
        old_text = Path(state['programme_text_path']).read_text(encoding='utf-8')
    material = old_text is None or shared_lines(old_text, page['text']) < MATERIAL
    state.update(verdict='published', programme_url=url, programme_chars=len(page['text']), programme_hash=h)
    if h != state.get('last_hash'):
        state['last_changed'] = today
    if 'first_published' not in state:
        state['first_published'] = today
        history.append({'date': today, 'verdict': 'published', 'url': url, 'hash': h[:12], 'chars': len(page['text'])})
    elif material and h != state.get('last_hash'):
        history.append({'date': today, 'verdict': 'changed', 'url': url, 'hash': h[:12], 'chars': len(page['text'])})
    state['material_change'] = material and h != state.get('last_hash')
    state['last_hash'] = h
    return state, {'url': url, 'page': page, 'hash': h, 'material': state['material_change']}


def store_document(conn, t: dict, found: dict, state: dict, today: str) -> str:
    """Writes the programme text and the document row of a new or materially changed version; returns the text path."""
    text = found['page']['text']
    folder = FOLDER / t['key']
    path = folder / f"{today}-{found['hash'][:12]}.txt"
    path.write_text(text, encoding='utf-8')
    state['programme_text_path'] = str(path)
    if conn is not None and found['material']:
        known = conn.execute('SELECT 1 FROM people WHERE id = ?', (t['person'],)).fetchone()
        if known:
            conn.execute(
                """INSERT INTO documents (person, source_type, source, external_id, url, title, date, fetched_at, sha256, path, licence,
                     attribution, status, chars) VALUES (?, 'programme', 'programme-watch', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'fetched', ?)
                   ON CONFLICT(source, external_id, person) DO NOTHING""",
                (t['person'], f"{t['key']}-{found['hash'][:12]}", found['url'], f"{t['label']} (programme 2027, relevé du {today})", today, now(),
                 sha256_file(path), rel(path), LICENCE, t.get('attribution', 'personnelle'), len(text)))
    return str(path)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--people', default='main')
    ap.add_argument('--db', help='database to record documents in (default $POSITIONS_DB or positions.sqlite)')
    ap.add_argument('--no-db', action='store_true', help='state file only')
    ap.add_argument('--today', help='override the date (tests)')
    args = ap.parse_args(argv)
    import people as people_mod
    from db import connect
    conn = connect(args.db) if args.db else connect()
    people_mod.sync(conn)
    ids = set(people_mod.resolve(conn, args.people))
    targets = [t for t in json.loads(TARGETS.read_text())['targets'] if t['person'] in ids]
    state = json.loads(STATE.read_text()) if STATE.exists() else {'_doc': 'Written by watch_programmes.py. One entry per target key.', 'targets': {}}
    today = args.today or now()[:10]
    fetcher = Fetcher(min_interval=2.0)
    summary = []
    for t in targets:
        log(f"== {t['person']} · {t['key']} · {t['url']}")
        entry, found = check_target(fetcher, t, state['targets'].get(t['key']), today)
        if found:
            store_document(None if args.no_db else conn, t, found, entry, today)
        entry['person'] = t['person']
        state['targets'][t['key']] = entry
        summary.append({'person': t['person'], 'key': t['key'], 'verdict': entry['verdict'], 'chars': entry.get('programme_chars') or entry.get('target_chars'),
                        'programme_url': entry.get('programme_url'), 'error': entry.get('error'), 'links': len(entry.get('links', []))})
    state['last_run'] = today
    RESEARCH.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(state, ensure_ascii=False, indent=1, sort_keys=True))
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main()
