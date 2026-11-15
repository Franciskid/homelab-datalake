# Vendored from the politico repository (scripts/pollslib/auto.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Generic extractor for first-round hypotheses and runoff duels in an institute report.

It reads every page that talks about voting intentions with the coordinate-based table reader (tables.py), picks the
column that holds the published figures by its heading, and keeps a table only when it passes the checks (known candidate
ids, sum 100 ± 2, enough candidates, "<1" cells coded 0.5 with the printed text kept). A table where the column cannot be
told from its heading is returned with `ambiguous: true` so the poll goes to review instead of being published.

This is the part that is less certain than the institute-specific parsers (elabe.py): its recall and precision per
institute are measured against the hand-checked records by scripts/polls-regression.py and reported in the README.
"""
import re

from . import tables as pt

PREFER = [r"publi", r"redress", r"pondere|pondér", r"^ensemble|total|inscrits"]
AVOID = r"brut|non redress|evolution|évolution|ecart|écart|rappel|precedent|précédent|variation|\+/-|vague"


def parse_cell(raw: str):
    raw = raw.strip().replace("%", "").strip()
    if raw.startswith("<"):
        return float(raw[1:].replace(",", ".")) / 2, raw
    if raw.startswith(">") or not re.search(r"\d", raw):
        return None, raw
    return float(raw.replace(",", ".")), raw


# Pages that look like voting intentions but are not the poll's own hypotheses: Ifop's reconstitution of the 2022 first round,
# and the tables broken down by party closeness (their columns are parties, not published results).
SKIP_PAGE = re.compile(r"reconstitution du 1er tour|proximite partisane|preference partisane|selon le sexe|par sexe")


def vote_pages(pdf, round_index: int) -> list[int]:
    pages = []
    n = pt.page_count(pdf)
    for p in range(1, n + 1):
        t = pt.norm(pt.page_text(pdf, p, layout=False))
        if "intention" not in t and "voteriez" not in t and "voterait" not in t:
            continue
        if SKIP_PAGE.search(t):
            continue
        runoff = bool(re.search(r"2nd tour|second tour|2eme tour|deuxieme tour|2e tour", t))
        if round_index == 1 and runoff or round_index == 0 and re.search(r"1er tour|premier tour|1e tour", t) and not runoff:
            pages.append(p)
    return pages


def _pick(table):
    heads = [pt.norm(h) for h in table["heads"]]
    n = len(table["cols"])
    ok = [j for j in range(n) if not re.search(AVOID, heads[j])]
    if n == 1:
        return [0], False, bool(re.search(PREFER[0] + "|" + PREFER[1] + "|" + PREFER[2], heads[0]))
    for k, pat in enumerate(PREFER):
        hit = [j for j in ok if re.search(pat, heads[j])]
        if len(hit) == 1:
            return hit, False, k < 3
    return ok, True, False  


def page_hypotheses(pdf, page: int, names: dict, round_index: int, alias_list=None):
    rows, ws = pt.table(pdf, page, names, alias_list)
    out = []
    for t in pt.segment(rows, ws):
        cols, ambiguous, confident = _pick(t)
        for j in cols:
            scores, raw = {}, {}
            for cid, box, vals in t["rows"]:
                if j >= len(vals) or not vals[j]:
                    continue
                v, r = parse_cell(vals[j])
                if v is None:
                    continue
                if cid in scores:
                    scores = {}
                    break
                scores[cid], raw[cid] = v, r
            total = sum(scores.values())
            need = 2 if round_index == 1 else 5
            if len(scores) < need or abs(total - 100) > 2 or (round_index == 1 and len(scores) != 2):
                continue
            out.append({"page": page, "scores": scores, "raw": raw, "ambiguous": ambiguous and len(cols) > 1, "confident": confident and not ambiguous,
                        "heading": t["heads"][j] if j < len(t["heads"]) else ""})
    return out


def extract(pdf, names: dict, round_index: int):
    """All table readings that pass the checks, in page order; exact duplicates (same scores) removed."""
    alias_list = pt.aliases(names)
    seen, out = set(), []
    for p in vote_pages(pdf, round_index):
        for h in page_hypotheses(pdf, p, names, round_index, alias_list):
            sig = tuple(sorted(h["scores"].items()))
            if sig in seen:
                continue
            seen.add(sig)
            out.append(h)
    return out
