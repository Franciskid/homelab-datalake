# Vendored from the politico repository (scripts/pollslib/crosstabs.py, ee9ed1d). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Cross-tabulations of other institutes' reports (Elabe's are in elabe.py).

Two layouts exist in the reports we hold:
  rows = categories, columns = candidates   (Ipsos, Ifop: "ENSEMBLE / SEXE / Hommes ... / ÂGE ...")
  rows = candidates, columns = categories   (OpinionWay: "Nathalie Arthaud  1%  1% ...")
A table is attached to a hypothesis of the poll only when its candidate set (and, when the table has a total row, its
total figures) match that hypothesis; anything else is left out and counted as not read.
"""
import re

from . import elabe, vocab
from . import tables as pt

TOK = r"(?:\d+(?:[.,]\d+)?%?|\*|-)"
ROW = re.compile(rf"^(?P<label>.*?)(?:\s{{2,}}|\s)(?P<nums>(?:{TOK}\s+)*{TOK})\s*$")


def cell(tok: str):
    tok = tok.strip().replace("%", "")
    if tok == "*":
        return 0.5, "*"
    if tok == "-":
        return 0.0, "-"
    return float(tok.replace(",", ".")), tok


def _clean(line: str) -> str:
    line = re.sub(r"[\ue000-\uf8ff▪•●■]", " ", line.replace("&nbsp;", " "))
    return re.sub(r"\s*\.{2,}\s*", "  ", line).rstrip()


def header_candidates(pdf, page: int, names: dict, alias_list=None) -> list[str]:
    """Candidates named above the figures of a page, left to right (names that have no figure beside them)."""
    rows, ws = pt.table(pdf, page, names, alias_list)
    heads = sorted(((box, cid) for cid, box, cells in rows if not [c for c in cells if re.search(r"\d", c[1])]), key=lambda h: h[0]["y0"])
    clusters, cur = [], []
    for h in heads:                                  # names printed in the page title ("Hypothèse : R. Glucksmann, ...") are another band
        if cur and h[0]["y0"] - cur[-1][0]["y0"] > 20:
            clusters.append(cur)
            cur = []
        cur.append(h)
    if cur:
        clusters.append(cur)
    best = max(clusters, key=lambda c: len({cid for _, cid in c}), default=[])
    seen, out = set(), []
    for box, cid in sorted(best, key=lambda h: (h[0]["x0"] + h[0]["x1"]) / 2):
        if cid not in seen:
            seen.add(cid)
            out.append(cid)
    return out


def category_rows(text: str, ncols: int) -> dict:
    """Rows of a category-by-candidate page: {'total': [..]|None, 'rows': [{section,label,values,raw}]}."""
    out, section, pending, total = [], None, [], None
    for line in text.split("\n"):
        l = _clean(line)
        s = l.strip()
        if not s or re.fullmatch(r"\d{1,3}", s) or s.startswith("©") or s.startswith("*"):
            continue
        m = ROW.match(l)
        if m and len(m.group("nums").split()) in (ncols, ncols + 1):
            toks = m.group("nums").split()
            vals = [cell(t) for t in toks]
            if len(toks) == ncols + 1 and abs(vals[-1][0] - 100) < 0.6:  # trailing TOTAL = 100 column
                vals = vals[:-1]
            elif len(toks) == ncols + 1:
                continue
            label = re.sub(r"^[.\s]+", "", " ".join(pending + [m.group("label").strip()])).strip()
            pending = []
            parts = re.split(r"\s{3,}", m.group("label").strip())
            if len(parts) == 2 and parts[0].isupper() and not pending:
                section, label = parts[0], parts[1]
            if not label or len(label) > 60 or re.search(r"Si le premier tour|TOTAL", label) or (total is None and not out and section is None and pt.norm(label) != "ensemble"):
                continue
            row = {"section": section, "label": label, "values": [v for v, _ in vals], "raw": [r for _, r in vals]}
            if pt.norm(label) in ("ensemble", "total", "ensemble des francais", "ensemble de l'echantillon") and total is None:
                total = row
            else:
                out.append(row)
        elif not re.search(r"\d", s) or re.fullmatch(r"[\w '’()ÉÈÊÂÎÔÀ./+-]+", s) and not re.search(rf"\s{{2,}}{TOK}\s*$", l):
            if re.search(elabe._CONT_END, s) or s.count("(") > s.count(")"):
                pending.append(s)
            elif s.isupper() or pt.norm(s).split(" ")[0] in ("sexe", "age", "profession", "statut", "niveau", "region", "agglomeration", "categorie", "vote", "proximite", "preference"):
                section, pending = s, []
            elif pending:
                pending.append(s)
    return {"total": total, "rows": out}


def attach_category_rows(pdf, text: str, page: int, poll: dict, names: dict, question="vote_intention_t1", alias_list=None):
    """Read one category-rows page of a first-round poll and attach it to the hypothesis whose candidates it shows."""
    cands = header_candidates(pdf, page, names, alias_list)
    if len(cands) < 5:
        return None
    pool = [s for s in poll["scenarios"] if set(s["scores"]) == set(cands)]
    if not pool:
        return None
    parsed = category_rows(text, len(cands))
    if not parsed["rows"]:
        return None
    sc = pool[0]
    if parsed["total"] and len(pool) > 1:
        tv = parsed["total"]["values"]
        pool = [s for s in pool if all(abs(s["scores"][c] - v) <= 1.0 for c, v in zip(cands, tv, strict=True))] or pool
        sc = pool[0]
    problems = []
    if parsed["total"]:
        for c, v in zip(cands, parsed["total"]["values"], strict=True):
            if abs(sc["scores"][c] - v) > 1.0:
                problems.append(f"Total {c}={v} differs from the hypothesis ({sc['scores'][c]})")
    groups = []
    for r in parsed["rows"]:
        cat = vocab.categorize(r["section"], r["label"])
        groups.append({"dimension": cat["dimension"], "category": cat["category"], "parent": cat["parent"], "label": vocab.display_label(cat["dimension"], cat["category"], r["label"]), "label_original": r["label"], "section": r["section"],
                       "known": cat["known"], "values": dict(zip(cands, r["values"], strict=True)), "raw": dict(zip(cands, r["raw"], strict=True))})
    return {"question": question, "scenario": sc["id"], "page": page, "base": None, "columns": cands,
            "total": dict(zip(cands, parsed["total"]["values"], strict=True)) if parsed["total"] else None,
            "groups": groups, "problems": problems, "part": "crosstab"}


# ---- OpinionWay: candidates in rows, categories in columns -----------------------------------

OW_DEMO = [("sexe", "Homme"), ("sexe", "Femme"), ("age", "18-24 ans"), ("age", "25-34 ans"), ("age", "35-49 ans"), ("age", "50-64 ans"),
           ("age", "65 ans et plus"), ("csp", "CSP +"), ("csp", "CSP -"), ("statut", "Inactif")]
OW_PROX = [("proximite", "La France insoumise"), ("proximite", "Parti socialiste"), ("proximite", "Les Ecologistes"), ("proximite", "Majorité présidentielle"),
           ("proximite", "Les Républicains"), ("proximite", "Rassemblement national"), ("proximite", "Sans préférence")]
OW_VOTE = [("vote_2022_t1", "Jean-Luc Mélenchon"), ("vote_2022_t1", "Yannick Jadot"), ("vote_2022_t1", "Emmanuel Macron"), ("vote_2022_t1", "Valérie Pécresse"),
           ("vote_2022_t1", "Éric Zemmour"), ("vote_2022_t1", "Marine Le Pen"), ("vote_2022_t1", "Abstention, blanc et nul"),
           ("vote_leg_2024", "NFP et alliés"), ("vote_leg_2024", "Ensemble et alliés"), ("vote_leg_2024", "LR et alliés"), ("vote_leg_2024", "RN et alliés"),
           ("vote_leg_2024", "Abstention, blanc et nul")]
OW_SECTION = [("PROXIMITÉ PARTISANE", OW_PROX, False), ("ÉLECTION PRÉSIDENTIELLE 2022", OW_VOTE, False), ("SEXE", OW_DEMO, True)]


def opinionway_page(text: str, names: dict, page: int, poll: dict, alias_list=None, question="vote_intention_t1"):
    """One OpinionWay crosstab page (candidate rows). Returns a crosstab record attached to the matching hypothesis, or None."""
    flat = text.replace("\n", " ")
    spec = next(((cols, tot) for key, cols, tot in OW_SECTION if key in flat), None)
    if not spec:
        return None
    cols, has_total = spec
    alias_list = alias_list or pt.aliases(names)
    rows = []
    for line in text.split("\n"):
        l = line.strip()
        m = re.match(rf"^(?P<name>[^\d%]+?)\s{{2,}}(?P<nums>(?:(?:\d+%|-)\s*)+)$", re.sub(r"(?<=[%\-])\s+(?=\d|-)", "  ", l)) or re.match(rf"^(?P<name>[^\d%]+?)\s{{2,}}(?P<nums>.*)$", l)
        if not m:
            continue
        toks = re.findall(r"\d+%|-", m.group("nums"))
        n = pt.norm(m.group("name"))
        cid = next((cid for a, cid in alias_list if n == a or n.endswith(" " + a)), None)
        if cid and len(toks) == len(cols) + (1 if has_total else 0):
            rows.append((cid, [cell(t) for t in toks]))
    if len(rows) < 5:
        return None
    # the first column is the poll's own published total when present: use it to find the hypothesis
    sc = None
    if has_total:
        totals = {cid: v[0][0] for cid, v in rows}
        for s in poll["scenarios"]:
            if set(s["scores"]) == set(totals) and all(abs(s["scores"][c] - totals[c]) <= 1.0 for c in totals):
                sc = s
                break
        if not sc:
            return None
    else:
        sets = [s for s in poll["scenarios"] if set(s["scores"]) <= {c for c, _ in rows} and len(s["scores"]) >= len(rows) - 1]
        h = re.search(r"HYPOTHÈSE (\d)", text)
        sc = next((s for s in poll["scenarios"] if h and s["id"].endswith(f"-primary-{h.group(1)}")), sets[0] if sets else None)
        if not sc:
            return None
    order = [c for c, _ in rows]
    groups = []
    for j, (section, label) in enumerate(cols):
        k = j + (1 if has_total else 0)
        cat = vocab.categorize(section, label)
        groups.append({"dimension": cat["dimension"] if cat["known"] else section, "category": cat["category"], "parent": cat["parent"], "label": vocab.display_label(cat["dimension"], cat["category"], label), "label_original": label, "section": section,
                       "known": True, "values": {cid: v[k][0] for cid, v in rows}, "raw": {cid: v[k][1] for cid, v in rows}})
    return {"question": question, "scenario": sc["id"], "page": page, "base": None, "columns": order, "total": {cid: v[0][0] for cid, v in rows} if has_total else None,
            "groups": groups, "problems": [], "part": "crosstab", "note": "candidate rows; '-' printed for nobody"}


# ---- camp questions --------------------------------------------------------------------------

def odoxa_good_candidate(text: str, page: int, names: dict, alias_list=None) -> dict | None:
    """Odoxa: 'Quelles personnalités feraient de bons candidats pour leur camp ?' (bon / mauvais / NSP, then good among the camp's sympathisants)."""
    if "bons candidats pour" not in text:
        return None
    alias_list = alias_list or pt.aliases(names)
    answers = []
    for line in text.split("\n"):
        pcts = re.findall(r"(\d{1,3})%", line)
        if len(pcts) < 3:
            continue
        head = re.sub(r"\s+", " ", pt.norm(line[: line.find(pcts[0] + "%")])).strip()
        cid = next((cid for a, cid in alias_list if head.endswith(a)), None)
        if not cid:
            continue
        vals = [float(x) for x in pcts]
        good, bad = vals[0], vals[1]
        if len(vals) >= 4:
            nsp, camp = vals[2], vals[3]
        else:  # the NSP figure wrapped to the next line
            nsp, camp = round(100 - good - bad, 1), vals[2]
        if abs(good + bad + nsp - 100) > 1.5:
            continue
        answers.append({"person": cid, "good": good, "bad": bad, "no_opinion": nsp, "good_among_own_camp": camp})
    if len(answers) < 6:
        return None
    return {"question": {"key": "good_candidate_for_camp", "wording": "Pour chacune des personnalités politiques suivantes, dites-nous si elle ferait un bon ou un mauvais candidat pour son camp à l'élection présidentielle",
                         "base": "Ensemble des Français ; dernière colonne : sympathisants du camp de la personnalité"},
            "page": page, "populations": [{"id": "ensemble", "label": "Ensemble des Français", "answers": answers}]}
