# Vendored from the politico repository (scripts/pollslib/elabe.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Elabe full report (the PowerPoint export published on elabe.fr) -> poll record and details.

The layout is the same from one wave to the next:
  p.2   Fiche technique (sample, dates)        recap pages: "Récapitulatif des intentions de vote du 1er tour" / "du 2nd tour"
  one page per configuration with "n'expriment aucune intention de vote"
  one "sûr de votre choix" page, optional question pages (intérêt, meilleur candidat pour représenter ...)
  "Détail des résultats": one pair of pages (socio-demographic, political) per configuration and per question.

Everything is read from the text layer (pdftotext -layout for cross-tab rows, -tsv word positions for the recap tables),
and the numbers are cross-checked against each other: the recap, the "Total" row of the cross-tab and the
configuration page must agree, otherwise the poll goes to review.
"""
import re

from . import tables as pt
from . import vocab

MONTHS = {"janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6, "juillet": 7, "aout": 8,
          "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12}
NUM = r"\d+(?:[.,]\d+)?"


def fnum(s: str) -> float:
    return float(s.replace(",", ".").replace(" ", ""))


def pages_of(text: str) -> list[str]:
    return text.split("\f")


def find_pages(pages: list[str], pattern: str) -> list[int]:
    rx = re.compile(pattern, re.I)
    return [i + 1 for i, p in enumerate(pages) if rx.search(pt.norm(p[:700]) if False else p[:900])]


def is_elabe_full_report(pages: list[str]) -> bool:
    head = pt.norm(" ".join(pages[:3]))
    return "elabe" in head and "fiche technique" in head and any("Récapitulatif des intentions de vote du 1er tour" in p for p in pages[:12])


def meta(pages: list[str]) -> dict:
    p2 = pages[1] if len(pages) > 1 else ""
    flat = re.sub(r"\s+", " ", p2)
    sample = re.search(r"Echantillon de ([\d  ]+) personnes", flat)
    reg = re.search(r"dont ([\d  ]+) inscrits", flat)
    d = re.search(r"du (\d{1,2})(?:er)?(?: (\w+))? au (\d{1,2})(?:er)? (\w+) (20\d\d)", flat)
    out = {"sample": int(re.sub(r"\D", "", sample.group(1))) if sample else None,
           "registered": int(re.sub(r"\D", "", reg.group(1))) if reg else None}
    if d:
        d1, m1, d2, m2, y = d.groups()
        m2n = MONTHS[pt.norm(m2)]
        m1n = MONTHS[pt.norm(m1)] if m1 else m2n
        y1 = int(y) - (1 if m1n > m2n else 0)
        out["start"] = f"{y1}-{m1n:02d}-{int(d1):02d}"
        out["end"] = f"{int(y)}-{m2n:02d}-{int(d2):02d}"
    head = re.sub(r"\s+", " ", pages[0]) if pages else ""
    c = re.search(r"Sondage Elabe pour (.+?) (\d{1,2}(?:er)? \w+ 20\d\d)", head)
    out["client"] = c.group(1) if c else None
    err = re.search(r"Marge d.erreur comprise entre ([\d,]+) et ([\d,]+) pts?", flat)
    out["margin"] = [fnum(err.group(1)), fnum(err.group(2))] if err else None
    return out


def _recap(pdf, page: int, names: dict, n_cfg: int | None = None):
    """Recap table: candidate rows x 'Configuration k' columns, by word position."""
    ws = pt.words(pdf, page)
    heads = []
    for i, w in enumerate(ws):
        if pt.norm(w["text"]) == "configuration" and i + 1 < len(ws) and re.fullmatch(r"\d", ws[i + 1]["text"]):
            heads.append((int(ws[i + 1]["text"]), (w["x0"] + ws[i + 1]["x1"]) / 2, w["yc"]))
    if not heads:
        return {}, {}, []
    hy = heads[0][2]
    rows, _ = pt.table(pdf, page, names)
    cols = {k: x for k, x, _ in heads}
    out = {k: {} for k in cols}
    raw = {k: {} for k in cols}
    order = [cid for cid, box, cells in sorted(rows, key=lambda r: r[1]["y0"]) if box["y0"] >= hy]
    for cid, box, cells in rows:
        if box["y0"] < hy:
            continue
        for x1, txt in cells:
            if not pt.is_num(txt) or txt.strip()[:1] in "<>":
                continue
            # a number is centred under its header; compare with the cell centre (x1 - width/2 ~ x1 - 6)
            k = min(cols, key=lambda k: abs(cols[k] - (x1 - 6 * len(txt) / 2)))
            if abs(cols[k] - (x1 - 6 * len(txt) / 2)) > 40:
                continue
            out[k][cid] = pt.value(txt)
            raw[k][cid] = txt.strip().replace("%", "")
    return out, raw, order


def configurations(pages: list[str]) -> dict:
    """{('1er'|'2nd', n): {'page': p, 'description': str, 'unexpressed': float|None}} from the per-configuration pages."""
    out = {}
    for i, p in enumerate(pages):
        m = re.match(r"\s*(1er|2nd) TOUR\s*-\s*CONFIGURATION (\d)\s+Voir marges", p)
        if not m:
            continue
        lines = [l.strip() for l in p.split("\n") if l.strip()]
        desc = []
        for l in lines[1:]:
            if l.startswith("Si le ") or l.startswith("En %"):
                break
            desc.append(re.sub(r"\s{2,}", " ", l))
        un = re.search(r"(\d+(?:[.,]\d+)?)\s*%\s*n.expriment", re.sub(r"\s+", " ", p))
        out[(m.group(1), int(m.group(2)))] = {"page": i + 1, "description": " / ".join(desc), "unexpressed": fnum(un.group(1)) if un else None}
    return out


def config_chart_scores(pdf, page: int, names: dict) -> dict:
    """Current-wave value of every candidate on a configuration page (first number right of the label)."""
    rows, _ = pt.table(pdf, page, names)
    out = {}
    for cid, box, cells in rows:
        nums = [(x, t) for x, t in cells if pt.is_num(t) and t.strip()[:1] not in "<>"]
        # chart pages: [bar label][Rappel previous] -> first printed number nearest the bar is the current wave
        if nums:
            out.setdefault(cid, []).append(nums)
    return out


# ---- cross-tabs -------------------------------------------------------------------------------

_ROW = re.compile(rf"^(?P<label>.*?)(?:\s{{2,}}|\s)(?P<nums>(?:{NUM}\s+)*{NUM})\s*$")
_CONT_END = re.compile(r"(?:\bet|\bde|\bd'|\bd’|\bses|,|\(|-)$", re.I)


def _dangling(label: str) -> bool:
    return bool(_CONT_END.search(label.strip())) or label.count("(") > label.count(")")


def _is_numeric_only(l: str) -> bool:
    return bool(re.fullmatch(rf"\s*(?:{NUM}\s+)*{NUM}\s*", l))


def parse_crosstab_page(text: str, ncols: int | None = None) -> dict:
    """Rows of a 'Détail des résultats' page: {'title','header':[...],'total':[..],'rows':[{section,label,values}]}.
    Wrapped labels (one or two label lines above or below the figures) are joined."""
    lines = [l.rstrip() for l in text.replace("&nbsp;", " ").split("\n")]
    title = ""
    for l in lines:
        if re.search(r"(?:TOUR\s*[–-]|\?\s*$|Selon vous|Diriez-vous|La prochaine)", l) and len(l.strip()) > 25:
            title = re.sub(r"\s{2,}", " ", l.strip()); break
    out_rows, section, pending, header = [], None, [], []
    prev_section, section_was_label_only = None, False
    started = False
    for idx, l in enumerate(lines):
        s = l.strip()
        if not s:
            continue
        if s == "x":
            started = True
            continue
        if re.match(r"(?:Les Français et|\d+\s+\d{1,2} \w+ 20\d\d)", s) or re.search(r"\d{1,2} \w+ 20\d\d$", s) and len(s.split()) <= 4:
            continue
        if not started and re.match(rf"Total\s+{NUM}", s):
            started = True
        if not started:
            if s.startswith("En %") or "En % -" in s or s.startswith("En%"):
                header = [h.strip() for h in re.split(r"\s{2,}", s) if h.strip()]
            elif header and not title.endswith(s):
                pass
            continue
        m = _ROW.match(l)
        if m and re.search(rf"\s{NUM}(?:\s|$)", " " + m.group("nums")) and (ncols is None or len(m.group("nums").split()) >= ncols or _is_numeric_only(l)):
            nums = [fnum(x) for x in m.group("nums").split()]
            label = " ".join(pending + ([m.group("label").strip()] if m.group("label").strip() else []))
            if not label and section and prev_section is not None and section_was_label_only:
                # the "section" line just read was the label of this figures row
                label, section = section, prev_section
            if ncols and len(nums) > ncols and not label:
                continue
            pending = []
            section_was_label_only = False
            if not label:
                label = "?"
            if label.lower() == "total":
                out_rows.append({"section": None, "label": "Total", "values": nums})
            else:
                out_rows.append({"section": section, "label": label, "values": nums})
        else:
            # label-only line: the end of a wrapped label above, a section title or the first line of a wrapped label
            if out_rows and _dangling(out_rows[-1]["label"]) and out_rows[-1].get("open", True):
                out_rows[-1]["label"] += " " + s
                out_rows[-1]["open"] = _dangling(out_rows[-1]["label"])
            elif _CONT_END.search(s) or s.count("(") > s.count(")") or (pending and not s[:1].isupper()):
                pending.append(s)
            elif pending:
                pending.append(s)
            else:
                prev_section, section, section_was_label_only = section, s, True
    # a wrapped label can also continue on the line below the figures ("Zemmour)")
    return {"title": title, "header": header, "rows": out_rows}


def _fix_continuations(rows: list[dict]) -> list[dict]:
    """Join a figure row whose label starts in lowercase or closes a parenthesis to the label before it when that one has none."""
    return rows


def crosstab_from_page(text: str, columns: list[str], question: str, scenario: str | None, page: int, base: str | None = None):
    """Parse one cross-tab page into the details schema; columns are the ids of the figures' columns, in order."""
    ncols = len(columns)
    raw = parse_crosstab_page(text, ncols)
    total = next((r for r in raw["rows"] if r["label"] == "Total"), None)
    groups, problems = [], []
    for r in raw["rows"]:
        if r["label"] == "Total":
            continue
        if len(r["values"]) != ncols:
            problems.append(f"{r['label']}: {len(r['values'])} figures, {ncols} expected")
            continue
        r["label"] = re.sub(r"(\w)- (\w)", r"\1-\2", r["label"])
        cat = vocab.categorize(r["section"], r["label"])
        groups.append({"dimension": cat["dimension"], "category": cat["category"], "parent": cat["parent"], "label": r["label"],
                       "section": r["section"], "known": cat["known"], "values": dict(zip(columns, r["values"], strict=True))})
    if total is None or len(total["values"]) != ncols:
        problems.append("no Total row with the expected figures")
    return {"question": question, "scenario": scenario, "page": page, "base": base, "columns": columns,
            "total": dict(zip(columns, total["values"], strict=True)) if total and len(total["values"]) == ncols else None,
            "groups": groups, "problems": problems}


# ---- other questions -------------------------------------------------------------------------

def certainty(text: str, names: dict, page: int, scenario: str, base: str | None) -> dict | None:
    """'Sûr de votre choix' / 'pourrait changer d'avis' per candidate and overall (one configuration)."""
    flat = text.replace("&nbsp;", " ")
    alias = pt.aliases(names)
    rows = []
    for line in flat.split("\n"):
        m = re.search(rf"({NUM})\s+\(([+-]?\d+)\)\s+({NUM})", line)
        if not m:
            continue
        n = pt.norm(line[:m.start()])
        hit = next((cid for a, cid in alias if re.search(r"(?<![\w-])" + re.escape(a) + r"\s*$", n)), None)
        if hit:
            rows.append({"candidate": hit, "sure": fnum(m.group(1)), "sure_delta": int(m.group(2)), "may_change": fnum(m.group(3))})
    ch = re.search(rf"\(([+-]?\d+)\)\*\s+({NUM})", flat)
    su = re.search(rf"^\s*({NUM})\s+\(([+-]\d+)\)\s*$", flat, re.M)
    if not rows:
        return None
    q = re.search(r"(Et diriez-vous.*?\?)", re.sub(r"\s+", " ", flat))
    out = {"question": {"key": "certainty", "wording": q.group(1) if q else None, "base": base}, "scenario": scenario, "page": page, "candidates": rows}
    if ch and su:
        out["overall"] = {"sure": fnum(su.group(1)), "sure_delta": int(su.group(2)), "may_change": fnum(ch.group(2)), "may_change_delta": int(ch.group(1))}
    return out


def camp_question(text: str, page: int) -> dict | None:
    """'Meilleur candidat pour représenter le centre et la droite': three populations, 1st citation and total citations."""
    flat = text.replace("&nbsp;", " ")
    q = re.search(r"(Selon vous, qui serait le meilleur candidat.*?\?)", re.sub(r"\s+", " ", flat))
    pops = [
        {"id": "sympathisants-centre-droite", "label": "Sympathisants Renaissance, MoDem, Horizons, UDI, Nouvelle Energie, LR", "measures": ["first", "total"]},
        {"id": "electeurs-macron-pecresse-2022", "label": "Électeurs E. Macron et V. Pécresse (présidentielle 2022, 1er tour)", "measures": ["first", "total"]},
        {"id": "ensemble", "label": "Ensemble des Français", "measures": ["total"]},
    ]
    share = re.findall(r"(\d+)% de l.échantillon", flat)
    for pop, sh in zip(pops[:2], share, strict=False):
        pop["share_of_sample"] = fnum(sh)
    name_rx = r"([A-Z]\.\s?[A-ZÉ][\w'’-]*(?:\s[A-ZÉ][\w'’-]*)?)"
    for line in flat.split("\n"):
        line = line.strip()
        if "Aucun d" in line:
            vals = [fnum(v) for v in re.findall(rf"Aucun d.entre eux\s+({NUM})", line)]
            for pop, v in zip(pops, vals, strict=False):
                pop["none"] = v
            continue
        parts = re.findall(rf"{name_rx}\s+({NUM})(?:\s+({NUM}))?(?=\s{{2,}}|\s*$)", line)
        if len(parts) < 2:
            continue
        for pop, (nm, a, b) in zip(pops, parts, strict=False):
            ans = {"person": nm.strip()}
            if pop["measures"] == ["first", "total"]:
                ans["first"], ans["total"] = fnum(a), fnum(b) if b else None
            else:
                ans["total"] = fnum(a)
            pop.setdefault("answers", []).append(ans)
    unans = re.findall(r"Sans-réponses\s*:\s*(\d+)%", flat)
    for pop, u in zip(pops[:2], unans, strict=False):
        pop["no_answer"] = fnum(u)
    if not all(p.get("answers") for p in pops):
        return None
    return {"question": {"key": "camp_leadership", "wording": q.group(1) if q else None,
                         "base": "Jusqu'à 3 réponses possibles à prioriser, en %", "camp": "centre-droite"}, "page": page, "populations": pops}


# ---- whole report -----------------------------------------------------------------------------

def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", pt.norm(name)).strip("-")


def person_resolver(names: dict, full_names: list[str]):
    """'F. Baroin' -> 'francois-baroin' (known id when the candidate list has them, else a slug of the full name)."""
    byinit = {}
    for full in full_names:
        parts = full.split()
        if len(parts) >= 2:
            byinit[(pt.norm(parts[0])[0], pt.norm(" ".join(parts[1:])))] = full
    inv = {pt.norm(v): k for k, v in names.items()}
    new = {}

    def resolve(short: str) -> str:
        m = re.match(r"([A-Z])\.\s?(.+)", short)
        full = byinit.get((pt.norm(m.group(1))[0], pt.norm(m.group(2)))) if m else None
        if not full:
            return slug(short)
        cid = inv.get(pt.norm(full)) or slug(full)
        if cid not in names:
            new[cid] = full
        return cid
    return resolve, new


def _duel_label(names, a, b):
    return f"{names[a]} / {names[b]}"


def build_report(pdf, text: str, names: dict, source: str) -> dict:
    """Returns {'meta','first','runoff','details','problems'}; 'first'/'runoff' are poll records (scenarios) or None."""
    pages = pages_of(text)
    m = meta(pages)
    problems = []
    if not (m.get("start") and m.get("end") and m.get("sample")):
        problems.append("fieldwork dates or sample size not found on the technical sheet")
    cfgs = configurations(pages)
    rec1 = next((i + 1 for i, p in enumerate(pages) if "Récapitulatif des intentions de vote du 1er tour" in p[:200]), None)
    rec2 = next((i + 1 for i, p in enumerate(pages) if "Récapitulatif des intentions de vote du 2nd tour" in p[:200]), None)
    pid = f"elabe_{m['start']}_{m['end']}_{m['sample']}"
    rid = f"elabe_{m['start']}_{m['end']}"
    base = "En % des intentions de vote exprimées (probabilité d'aller voter notes 8 à 10)"
    details = {"scenarios": {}, "crosstabs": [], "certainty": [], "camp_questions": [], "other_questions": []}
    first_sc, run_sc = [], []
    cfg_ids = {}
    if rec1:
        scores, raw, order = _recap(pdf, rec1, names)
        orders = {}
        for k in sorted(scores):
            sc = scores[k]
            if not sc:
                continue
            sid = f"{pid}-primary-{k}"
            cfg_ids[("1er", k)] = sid
            ordered = [c for c in order if c in sc]
            orders[sid] = ordered
            first_sc.append({"id": sid, "label": f"Hypothèse {k} · p. {rec1}", "page": rec1, "scores": {c: sc[c] for c in ordered},
                             "raw": {c: raw[k][c] for c in ordered}, "incomplete": False})
            info = cfgs.get(("1er", k))
            if info:
                details["scenarios"][sid] = {"description": info["description"], "no_expression_pct": info["unexpressed"], "page": info["page"]}
    if rec2:
        scores, raw, order = _recap(pdf, rec2, names)
        for k in sorted(scores):
            sc = scores[k]
            if not sc:
                continue
            ordered = [c for c in order if c in sc]
            if len(ordered) != 2:
                problems.append(f"runoff configuration {k}: {len(ordered)} candidates, 2 expected")
                continue
            sid = f"{rid}-p{rec2}" + ("" if k == 1 else f"-{k}")
            cfg_ids[("2nd", k)] = sid
            run_sc.append({"id": sid, "label": _duel_label(names, *ordered), "page": rec2, "scores": {c: sc[c] for c in ordered},
                           "raw": {c: raw[k][c] for c in ordered}, "incomplete": False})
            info = cfgs.get(("2nd", k))
            if info:
                details["scenarios"][sid] = {"description": info["description"], "no_expression_pct": info["unexpressed"], "page": info["page"]}
    # cross-tabs: one socio-demographic page and one political page per configuration
    col_ids = {sid: list(s["scores"]) for s in first_sc + run_sc for sid in [s["id"]]}
    for i, p in enumerate(pages):
        if "Détail des résultats" not in p[:200]:
            continue
        mt = re.search(r"(1er|2nd) TOUR\s*[–-]\s*CONFIGURATION (\d)", p)
        if mt:
            sid = cfg_ids.get((mt.group(1), int(mt.group(2))))
            if not sid:
                problems.append(f"p.{i+1}: cross-tab of a configuration that has no recap row")
                continue
            ct = crosstab_from_page(p, col_ids[sid], "vote_intention_t1" if mt.group(1) == "1er" else "vote_intention_t2", sid, i + 1, base)
            ct["part"] = "sociodemographique" if "sociodémographiques" in p[:200] else "politique"
            details["crosstabs"].append(ct)
    # generic questions (interest, camp leadership): header of the cross-tab gives the answers
    people_full = []
    for i, p in enumerate(pages):
        if "Détail des résultats" not in p[:200] or re.search(r"(1er|2nd) TOUR\s*[–-]\s*CONFIGURATION", p):
            continue
        raw = parse_crosstab_page(p)
        hdr = [h for h in raw["header"] if not h.startswith("En %")]
        total = next((r for r in raw["rows"] if r["label"] == "Total"), None)
        if not total or len(hdr) != len(total["values"]):
            problems.append(f"p.{i+1}: question page whose header ({len(hdr)}) does not match its columns")
            continue
        key = "camp_leadership" if "meilleur candidat" in p else "campaign_interest" if "intéressé" in p[:600] else "question_" + str(i + 1)
        cols = [slug(h) for h in hdr]
        ct = crosstab_from_page(p, cols, key, None, i + 1, "Ensemble des Français")
        ct["answer_labels"] = dict(zip(cols, hdr, strict=True))
        ct["part"] = "sociodemographique" if "sociodémographiques" in p[:200] else "politique"
        ct["question_wording"] = raw["title"]
        details["crosstabs"].append(ct)
        if key == "camp_leadership":
            people_full = [h for h in hdr if h != "Aucun d'entre eux"]
    resolve, newpeople = person_resolver(names, people_full)
    for i, p in enumerate(pages):
        if "sûr(e) de votre choix" in p and "Et diriez-vous" in p:
            mt = re.search(r"CONFIGURATION (\d)", p[:200])
            sid = cfg_ids.get(("1er", int(mt.group(1)))) if mt else None
            c = certainty(p, names, i + 1, sid, "En % des intentions de vote (probabilité d'aller voter notes 8 à 10)")
            if c:
                details["certainty"].append(c)
            else:
                problems.append(f"p.{i+1}: certainty page not parsed")
        if "Selon vous, qui serait le meilleur candidat pour représenter" in p and "Détail" not in p[:200] and "Principaux enseignements" not in p[:200]:
            c = camp_question(p, i + 1)
            if c:
                for pop in c["populations"]:
                    for a in pop["answers"]:
                        a["person"] = resolve(a["person"])
                details["camp_questions"].append(c)
            else:
                problems.append(f"p.{i+1}: camp question not parsed")
        if "intéressé ou pas intéressé" in p and "Détail" not in p[:200]:
            flat = re.sub(r"\s+", " ", p)
            q = re.search(r"(La prochaine élection.*?\?)", flat)
            tot = {pt.norm(a): (fnum(v), d) for a, v, d in re.findall(rf"TOTAL ((?:PAS )?INTERESSE)(?: \d+)* ({NUM})% \(([^)]*)\)", flat)}
            for ct in details["crosstabs"]:
                if ct["question"] == "campaign_interest" and ct.get("part") == "sociodemographique":
                    ans = []
                    for c in ct["columns"]:
                        lab = ct["answer_labels"][c]
                        d = tot.get(pt.norm(re.sub(r"^TOTAL ", "", lab)), (None, None))[1] if lab.startswith("TOTAL") else None
                        d = "0" if d == "=" else d
                        ans.append({"label": lab, "value": ct["total"][c], **({"delta_pt": d} if d else {})})
                    details["other_questions"].append({"question": {"key": "campaign_interest", "wording": q.group(1) if q else None, "base": "Ensemble des Français"},
                                                       "page": i + 1, "answers": ans, "crosstab_page": ct["page"]})
    details["people"] = newpeople
    poll_common = {"institute": "Elabe", "start": m.get("start"), "end": m.get("end"), "sample": m.get("sample"), "source": source,
                   "extraction": "primary-pdf", "client": m.get("client")}
    first = {"id": pid, **poll_common, "scenarios": first_sc, "reviewed": False} if first_sc else None
    runoff = ({"id": rid, **poll_common, "registered": m.get("registered"), "scenarios": run_sc, "reviewed": False} if run_sc else None)
    return {"meta": m, "first": first, "runoff": runoff, "details": details, "problems": problems, "pages": len(pages),
            "recap_pages": {"first": rec1, "runoff": rec2}}


def notice_certainty(text: str, poll: dict, names: dict) -> list[dict]:
    """Elabe's Commission-des-sondages notice ("QUESTIONS PUBLIÉES"): overall 'sûr de mon choix' share after a hypothesis."""
    pages = pages_of(text)
    alias = pt.aliases(names)
    out = []
    for pi, page in enumerate(pages, 1):
        if "QUESTIONS PUBLI" not in page or "sûr(e) de mon choix" not in page:
            continue
        block, last = None, None
        for line in page.replace("&nbsp;", " ").split("\n"):
            s = line.strip()
            if re.search(r"pour lequel des candidats voteriez-vous", s) or re.search(r"Si le 1er tour avait lieu", s):
                block = {}
                continue
            m = re.match(rf"(.+?)\s{{2,}}({NUM})\s*$", line.strip())
            if block is not None and m:
                n = pt.norm(m.group(1))
                cid = next((cid for a, cid in alias if n == a), None)
                if cid:
                    block[cid] = fnum(m.group(2))
                    continue
            if block and re.search(r"N'expriment", s):
                last = dict(block)
                block = None
            sure = re.search(rf"Je suis sûr\(e\) de mon choix\s+({NUM})", s)
            chg = re.search(rf"Je pourrais changer d.avis[^\d]*({NUM})\s*$", s)
            if sure and last:
                last["_sure"] = fnum(sure.group(1))
            if chg and last and "_sure" in last:
                scores = {k: v for k, v in last.items() if not k.startswith("_")}
                sc = next((s_ for s_ in poll["scenarios"] if set(s_["scores"]) == set(scores) and all(abs(s_["scores"][c] - v) < 0.01 for c, v in scores.items())), None)
                if sc:
                    out.append({"question": {"key": "certainty", "wording": "Et diriez-vous que vous êtes sûr(e) de votre choix ou que vous pourriez changer d'avis d'ici à l'élection ?",
                                             "base": "Intentions de vote exprimées"}, "scenario": sc["id"], "page": pi,
                                "overall": {"sure": last["_sure"], "may_change": fnum(chg.group(1))}, "candidates": []})
                last = None
    return out
