# Vendored from the politico repository (scripts/pollslib/tables.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Coordinate-based reader for poll tables in primary PDF reports.

pdftotext -tsv gives every word with its bounding box. Candidate names are
matched on the reading-order text stream (so wrapped names such as
"Raphaël / 11,0 / Glucksmann" or "Nicolas Dupont- / Aignan" still match), and
the numbers printed to the right of the name, inside its vertical extent, form
the row. Numbers are kept exactly as printed (raw) next to their float value.

PDFs are untrusted data: they are only read through pdftotext.
"""
import json, re, subprocess, unicodedata, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
NUM = re.compile(r'^(?:<|>)?\d{1,3}(?:[.,]\d{1,2})?%?$')
DASH = {'-', '–', '—', '−', '/', 'nt', 'n.t.', '*'}


def norm(s):
    s = s.lower().replace('œ', 'oe').replace('’', "'")
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def load_candidates():
    idx = json.loads((ROOT / 'research/primary/discovery-index.json').read_text())
    names = dict(idx['candidates'])
    rec = json.loads((ROOT / 'research/primary/extracted-records.json').read_text())
    for r in rec['rounds']:
        names.update(r['candidates'])
    return names


# Extra aliases: surnames as printed in tables that omit first names, and
# spellings seen in reports. Keys are candidate ids.
EXTRA = {
    'marine-le-pen': ['m. le pen', 'marine le pen', 'le pen'],
    'jean-luc-melenchon': ['melenchon', 'j-l. melenchon', 'jl melenchon'],
    'edouard-philippe': ['e. philippe', 'edouard philippe'],
    'nicolas-dupont-aignan': ['dupont-aignan', 'dupont aignan'],
    'vote-blanc': ['vote blanc', 'voterait blanc', 'voteriez blanc', 'blanc ou nul', 'vote blanc ou nul'],
    'dominique-de-villepin': ['de villepin', 'villepin'],
    'philippe-de-villiers': ['de villiers', 'villiers'],
}


def aliases(names):
    out = []
    for cid, name in names.items():
        n = norm(name)
        forms = {n}
        parts = n.split()
        if len(parts) >= 2 and cid != 'vote-blanc':
            forms.add(parts[0][0] + '. ' + ' '.join(parts[1:]))
            sur = ' '.join(parts[1:])
            forms.add(sur)
        forms.update(EXTRA.get(cid, []))
        for f in forms:
            out.append((f, cid))
    out.sort(key=lambda x: -len(x[0]))
    return out


def words(pdf, page):
    tsv = subprocess.run(['pdftotext', '-tsv', '-f', str(page), '-l', str(page), str(pdf), '-'],
                         capture_output=True, text=True, check=True).stdout
    out = []
    for line in tsv.splitlines()[1:]:
        f = line.split('\t')
        if len(f) < 12 or f[0] != '5':
            continue
        l, t, w, h = map(float, f[6:10])
        out.append({'x0': l, 'x1': l + w, 'y0': t, 'y1': t + h, 'yc': t + h / 2, 'text': f[11],
                    'line': (int(f[2]), int(f[3]), int(f[4]))})
    return out


def page_count(pdf):
    info = subprocess.run(['pdfinfo', str(pdf)], capture_output=True, text=True).stdout
    return int(re.search(r'Pages:\s+(\d+)', info).group(1))


def page_text(pdf, page, layout=True):
    args = ['pdftotext'] + (['-layout'] if layout else []) + ['-f', str(page), '-l', str(page), str(pdf), '-']
    return subprocess.run(args, capture_output=True, text=True).stdout


def is_num(t):
    return bool(NUM.match(t.strip()))


def value(t):
    t = t.strip().replace('%', '')
    if not re.search(r'\d', t):
        return None
    if t[:1] in '<>':
        return None
    return float(t.replace(',', '.'))


def table(pdf, page, names, alias_list=None):
    """Return rows [(cid, label_box, [(x1, raw)])] for every candidate name found."""
    alias_list = alias_list or aliases(names)
    ws = words(pdf, page)
    text_ws = [w for w in ws if not is_num(w['text']) and w['text'] not in DASH]
    # Build joined normalized stream with char offsets -> word index.
    stream, owner = '', []
    for i, w in enumerate(text_ws):
        tok = norm(w['text'])
        if stream and not stream.endswith('-'):
            stream += ' '; owner.append(None)
        for ch in tok:
            stream += ch; owner.append(i)
    used = [False] * len(stream)
    hits = []
    for a, cid in alias_list:
        for m in re.finditer(r'(?<![\w-])' + re.escape(a) + r'(?![\w-])', stream):
            s, e = m.span()
            if any(used[s:e]):
                continue
            idxs = sorted({owner[k] for k in range(s, e) if owner[k] is not None})
            box = [text_ws[k] for k in idxs]
            # Name must sit on at most 3 consecutive lines and be horizontally compact.
            if max(b['y1'] for b in box) - min(b['y0'] for b in box) > 40:
                continue
            for k in range(s, e):
                used[k] = True
            hits.append((cid, {'x0': min(b['x0'] for b in box), 'x1': max(b['x1'] for b in box),
                               'y0': min(b['y0'] for b in box), 'y1': max(b['y1'] for b in box)}))
    nums = [w for w in ws if is_num(w['text']) or w['text'] in ('-', '–', '—', '−', 'nt')]
    rows = []
    for cid, box in hits:
        lo, hi = box['y0'] - 2, box['y1'] + 2
        # Stop at the next label on the same line (side-by-side tables).
        limit = min([o['x0'] for _, o in hits if o is not box and o['x0'] > box['x1']
                     and o['y0'] < box['y1'] and box['y0'] < o['y1']] or [1e9])
        cells = [n for n in nums if box['x1'] - 1 < n['x0'] < limit and lo <= n['yc'] <= hi]
        rows.append([cid, box, cells])
    # A number in two rows' ranges belongs to the row whose centre is closest.
    for n in nums:
        owners = [r for r in rows if n in r[2]]
        if len(owners) > 1:
            best = min(owners, key=lambda r: abs((r[1]['y0'] + r[1]['y1']) / 2 - n['yc']))
            for r in owners:
                if r is not best:
                    r[2].remove(n)
    return [(cid, box, sorted(((c['x1'], c['text']) for c in cells))) for cid, box, cells in rows], ws


def columns(rows, tol=12):
    """Cluster number right edges into columns; returns sorted column x positions."""
    xs = sorted(x for _, _, cells in rows for x, raw in cells if re.search(r'\d', raw))
    cols = []
    for x in xs:
        if cols and x - cols[-1][-1] <= tol:
            cols[-1].append(x)
        else:
            cols.append([x])
    return [sum(c) / len(c) for c in cols if len(c) >= 3]


def grid(rows, cols, tol=12):
    out = []
    for cid, box, cells in rows:
        vals = [None] * len(cols)
        for x, raw in cells:
            j = min(range(len(cols)), key=lambda j: abs(cols[j] - x)) if cols else None
            if j is not None and abs(cols[j] - x) <= tol * 1.5 and vals[j] is None:
                vals[j] = raw
        out.append((cid, box, vals))
    return out


def pt_marker(w):
    """Words that open a new stacked table ("2ND TOUR", "CONFIGURATION 2", "Hypothèse 3")."""
    return norm(w['text']).strip(':') in ('tour', 'configuration', 'hypothese')


def segment(rows, ws):
    """Split candidate rows into separate tables (side-by-side or stacked)."""
    rows = [r for r in rows if r[2]]
    if not rows:
        return []
    # Cluster by label left edge into vertical strips.
    strips = []
    for r in sorted(rows, key=lambda r: r[1]['x0']):
        # Same strip when the label boxes overlap horizontally.
        homes = [st for st in strips if any(r[1]['x0'] < o[1]['x1'] - 3 and o[1]['x0'] < r[1]['x1'] - 3 for o in st)]
        merged = [r] + [o for st in homes for o in st]
        strips = [st for st in strips if not any(st is h for h in homes)] + [merged]
    strips.sort(key=lambda st: min(o[1]['x0'] for o in st))
    lefts = [min(r[1]['x0'] for r in s) for s in strips]
    tables = []
    for si, s in enumerate(strips):
        right_bound = lefts[si + 1] - 2 if si + 1 < len(lefts) else 1e9
        s = sorted(s, key=lambda r: r[1]['y0'])
        gaps = [b[1]['y0'] - a[1]['y0'] for a, b in zip(s, s[1:])]
        med = sorted(gaps)[len(gaps) // 2] if gaps else 0
        cur = []
        for i, r in enumerate(s):
            gap = r[1]['y0'] - s[i - 1][1]['y0'] if i else 0
            prev = s[i - 1] if i else None
            marker = prev is not None and any(
                pt_marker(w) and prev[1]['y1'] - 1 < w['yc'] < r[1]['y0'] + 1
                and min(o[1]['x0'] for o in s) - 20 <= w['x0'] <= max(o[1]['x1'] for o in s) for w in ws)
            if cur and (marker or r[0] in {c[0] for c in cur} or (med and gap > 2.6 * med and gap > 30)):
                tables.append(cur); cur = []
            cur.append(r)
        if cur:
            tables.append(cur)
        for t in tables:
            for r in t:
                r[2][:] = [c for c in r[2] if c[0] < right_bound + 40 or right_bound == 1e9]
    out = []
    for t in tables:
        cols = columns(t, tol=12)
        g = grid(t, cols) if cols else []
        filled = sum(1 for _, _, v in g if any(v))
        if not cols or filled < 0.6 * len(t):
            # Bar chart: one printed value per row, at the end of each bar.
            t = [(cid, box, [c for c in cells if re.search(r'\d', c[1])]) for cid, box, cells in t]
            xs = [cells[0][0] for _, _, cells in t if cells]
            cols = [sum(xs) / len(xs)] if xs else []
            g = [(cid, box, [cells[0][1] if cells else None]) for cid, box, cells in t]
        top = min(r[1]['y0'] for r in t); x0 = min(r[1]['x0'] for r in t)
        xr = max(cols) + 5 if cols else max(r[1]['x1'] for r in t)
        head = [w for w in ws if top - 90 < w['y1'] <= top + 1 and w['x1'] >= x0 - 30 and w['x0'] <= xr + 30]
        heads = []
        for c in cols:
            hw = [w['text'] for w in head if abs(w['x1'] - c) < 40 or (w['x0'] - 10 <= c <= w['x1'] + 30)]
            heads.append(' '.join(hw[-6:]))
        cells = {r[0]: r[2] for r in t}
        out.append({'cells': cells, 'top': top, 'x0': x0, 'cols': cols, 'heads': heads, 'rows': g,
                    'title': ' '.join(w['text'] for w in head)[-200:]})
    return out
