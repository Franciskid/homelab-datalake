# Vendored from the politico repository (scripts/pollslib/pipeline.py, 6db2c47). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""From a downloaded report to validated poll records: parse, validate, de-duplicate, classify, merge.

Used by the homelab datalake's daily job (vendored copy) and by scripts/polls-ingest.py in this repository.
Nothing here touches the network: the caller downloads, this module reads PDFs through pdftotext only.
"""
import copy
import re
import subprocess
from datetime import date, datetime, timezone

from . import auto, elabe, validate, yougov
from . import tables as pt

DETAILS_VERSION = 1
# Parsers whose result is published without review when every check passes.
AUTO_PARSERS = {"elabe-full", yougov.FORMAT}
# Generic table reader: first-round hypotheses of these institutes passed the regression against hand-checked records
# (scripts/polls-regression.py). Anything else, and every runoff read this way, is review-needed.
AUTO_GENERIC_INSTITUTES = {"Ifop", "OpinionWay", "YouGov"}


def pdf_text(pdf) -> str:
    return subprocess.run(["pdftotext", "-layout", str(pdf), "-"], capture_output=True, text=True, check=True).stdout


MONTHS = elabe.MONTHS


def generic_meta(text: str, published: str | None = None) -> dict:
    """Fieldwork dates and sample size from the report's text (technical sheet). `published` (ISO, from the notice) completes
    a "du 3 au 5" whose month sits elsewhere on the page."""
    flat = re.sub(r"\s+", " ", text[:20000])
    out = {}
    d = re.search(r"du (\d{1,2})(?:er)?(?: ([^\W\d_]+))? au (\d{1,2})(?:er)?(?: ([^\W\d_]+) (20\d\d))?", flat, re.I)
    if d:
        d1, m1, d2, m2, y = d.groups()
        if not (m2 and y):
            after = re.search(r"([^\W\d_]+) (20\d\d)", flat[d.end():d.end() + 160])
            if after and MONTHS.get(pt.norm(after.group(1))):
                m2, y = after.groups()
            elif published:
                pd = date.fromisoformat(published)
                m2n_ = pd.month if int(d2) <= pd.day else (pd.month - 2) % 12 + 1
                m2, y = next(k for k, v in MONTHS.items() if v == m2n_), str(pd.year - (1 if m2n_ > pd.month else 0))
        try:
            m2n = MONTHS[pt.norm(m2 or "")]
            m1n = MONTHS[pt.norm(m1)] if m1 and pt.norm(m1) in MONTHS else m2n
            out["start"] = f"{int(y) - (1 if m1n > m2n else 0)}-{m1n:02d}-{int(d1):02d}"
            out["end"] = f"{int(y)}-{m2n:02d}-{int(d2):02d}"
        except (KeyError, TypeError, ValueError):
            pass
    s = re.search(r"(?:échantillon|echantillon)[^.]{0,80}?(?:de|:)\s*([\d  .]{3,7})\s*(?:personnes|inscrits|Français|répondants|individus)", flat, re.I)
    if s:
        out["sample"] = int(re.sub(r"\D", "", s.group(1)))
    return out


def make_id(institute: str, start: str, end: str, sample: int | None, round_index: int) -> str:
    base = f"{pt.norm(institute).replace(' ', '')}_{start}_{end}"
    return base + (f"_{sample}" if round_index == 0 and sample else "")


def parse_document(pdf, text: str, institute: str, source: str, names: dict, hint: dict | None = None) -> dict:
    """Returns {'parser','polls':[(round_index, poll)], 'details': {...}, 'problems': [...]}.
    `hint` may carry start/end/sample/client from the notice when the report's own text lacks them."""
    pages = elabe.pages_of(text)
    hint = hint or {}
    if institute.lower() == "elabe" and elabe.is_elabe_full_report(pages):
        r = elabe.build_report(pdf, text, names, source)
        polls = [(0, r["first"])] * bool(r["first"]) + [(1, r["runoff"])] * bool(r["runoff"])
        return {"parser": "elabe-full", "polls": polls, "details": r["details"], "problems": r["problems"], "pages": r["pages"]}
    m = {**generic_meta(text, hint.get('published')), **{k: v for k, v in hint.items() if v and k != 'published'}}
    problems = []
    if not (m.get("start") and m.get("end") and m.get("sample")):
        problems.append("fieldwork dates or sample size not found; give them in the notice")
    if institute.lower() == "yougov" and yougov.is_notice(pages):
        sample, registered = yougov.sample_sizes(pages)
        problems = []
        if not (m.get("start") and m.get("end") and sample and 0 < registered <= sample):
            problems.append("YouGov notice: fieldwork dates or sample sizes missing")
        rounds, table_problems = yougov.read_pages(pages, names, sample)
        polls = []
        for ri, hypotheses in enumerate(rounds):
            if not hypotheses:
                continue
            pid = make_id(institute, m.get("start", "?"), m.get("end", "?"), sample, ri)
            scenarios = [{**s, "id": f"{pid}-h{n}"} for n, s in sorted(hypotheses.items())]
            polls.append((ri, {"id": pid, "institute": institute, "start": m.get("start"), "end": m.get("end"),
                               "sample": sample, "registered": registered, "client": "Le HuffPost", "source": source,
                               "scenarios": scenarios, "extraction": "primary-pdf", "reviewed": False}))
        return {"parser": yougov.FORMAT, "polls": polls, "details": None,
                "problems": problems + table_problems}
    polls = []
    flags = {"ambiguous": False, "unconfident": False}
    for ri in (0, 1):
        hyps = auto.extract(pdf, names, ri)
        if not hyps:
            continue
        pid = make_id(institute, m.get("start", "?"), m.get("end", "?"), m.get("sample"), ri)
        sc = []
        for i, h in enumerate(hyps, 1):
            flags["ambiguous"] |= h["ambiguous"]
            flags["unconfident"] |= not h["confident"]
            label = f"Hypothèse {i} · p. {h['page']}" if ri == 0 else " / ".join(names[c] for c in h["scores"]) 
            sc.append({"id": f"{pid}-primary-{i}" if ri == 0 else f"{pid}-p{h['page']}" + ("" if i == 1 else f"-{i}"), "label": label, "page": h["page"],
                       "scores": h["scores"], "raw": h["raw"], "incomplete": False})
        poll = {"id": pid, "institute": institute, "start": m.get("start"), "end": m.get("end"), "sample": m.get("sample"), "source": source,
                "scenarios": sc, "extraction": "primary-pdf", "reviewed": False}
        if m.get("client"):
            poll["client"] = m["client"]
        polls.append((ri, poll))
    return {"parser": "generic-tables", "polls": polls, "details": None, "problems": problems, "pages": len(pages), "flags": flags}


def is_duplicate(poll: dict, existing: list[dict]) -> dict | None:
    """The poll already in `existing`: same id, or same institute with the same fieldwork dates, or the same first scores."""
    first = poll["scenarios"][0]["scores"] if poll.get("scenarios") else None
    if not poll.get("end") or not poll.get("start"):
        return None
    for e in existing:
        if e["id"] == poll["id"]:
            return e
        if e["institute"] == poll["institute"] and e["start"] == poll["start"] and e["end"] == poll["end"]:
            return e
        if first and e["institute"] == poll["institute"] and abs((date.fromisoformat(e["end"]) - date.fromisoformat(poll["end"])).days) <= 1 \
                and any(s["scores"] == first for s in e["scenarios"]):
            return e
    return None


def classify(parsed: dict, round_index: int, poll: dict, candidates: dict, details_errors: list[str], today: date | None = None,
             reference: list[dict] | None = None) -> tuple[str, list[str]]:
    """('auto' | 'review', reasons). Auto needs a trusted parser and no error of any kind."""
    reasons = list(parsed.get("problems", [])) + validate.poll_problems(poll, round_index, candidates, today) + details_errors
    if reference:
        reasons += validate.plausibility(poll, reference, threshold=12.0 if round_index == 1 else 10.0)
    if parsed["parser"] in AUTO_PARSERS:
        trusted = True
    else:
        f = parsed.get("flags", {})
        trusted = (round_index == 0 and poll["institute"] in AUTO_GENERIC_INSTITUTES and not f.get("ambiguous") and not f.get("unconfident"))
        if not trusted:
            reasons.append("generic table reader: " + ("runoff duels are always reviewed" if round_index == 1 else
                           "institute not trusted for automatic publication" if poll["institute"] not in AUTO_GENERIC_INSTITUTES else
                           "a table's published column was ambiguous"))
    return ("auto" if trusted and not reasons else "review"), reasons


def stamp(poll: dict, mode: str, parser: str, status: str, reasons: list[str], now: datetime | None = None) -> dict:
    poll = copy.deepcopy(poll)
    poll["ingest"] = {"mode": mode, "parser": parser, "status": status, "at": (now or datetime.now(timezone.utc)).isoformat(timespec="seconds")}
    if reasons:
        poll["ingest"]["reasons"] = reasons
    return poll


def details_for(details, poll):
    """The part of a report's details that belongs to this poll (its scenarios, plus the report-wide questions on the first-round poll)."""
    if not details:
        return None
    ids = {s["id"] for s in poll["scenarios"]}
    first_round = poll["id"].split("_")[-1].isdigit()
    out = {"scenarios": {k: v for k, v in details.get("scenarios", {}).items() if k in ids},
           "crosstabs": [c for c in details.get("crosstabs", []) if c.get("scenario") in ids or (first_round and c.get("scenario") is None)]}
    if first_round:
        for k in ("certainty", "camp_questions", "other_questions"):
            out[k] = details.get(k, [])
        out["people"] = details.get("people", {})
    return out


