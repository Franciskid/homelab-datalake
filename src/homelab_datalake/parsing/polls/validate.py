# Vendored from the politico repository (scripts/pollslib/validate.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""Validation of poll records and their details; the verdict that decides auto-publication.

A poll is `auto` only when it has no error. Warnings (unknown category labels, a cross-tab total that drifts by rounding)
are listed but do not block; errors send the poll to the review list. Nothing here edits the data.
"""
import re
from datetime import date

ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _d(s):
    try:
        return date.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def poll_problems(poll: dict, round_index: int, candidates: dict, today: date | None = None) -> list[str]:
    """Errors on a poll record of rounds[round_index] (0 = first round, 1 = runoff)."""
    errs = []
    for k in ("id", "institute", "start", "end", "source"):
        if not poll.get(k):
            errs.append(f"missing {k}")
    s, e = _d(poll.get("start")), _d(poll.get("end"))
    if not (ISO.match(str(poll.get("start", ""))) and s):
        errs.append("start is not a valid ISO date")
    if not (ISO.match(str(poll.get("end", ""))) and e):
        errs.append("end is not a valid ISO date")
    if s and e:
        if s > e:
            errs.append("start after end")
        if (e - s).days > 20:
            errs.append(f"fieldwork lasts {(e - s).days} days")
        if e < date(2022, 5, 1):
            errs.append("fieldwork before the 2022 election")
        if today and e > today:
            errs.append("fieldwork in the future")
    n = poll.get("sample")
    if not isinstance(n, int) or not 300 <= n <= 50000:
        errs.append(f"sample size {n!r} missing or implausible")
    if not str(poll.get("source", "")).startswith("https://"):
        errs.append("source is not https")
    sc = poll.get("scenarios") or []
    if not sc:
        errs.append("no hypothesis extracted")
    seen_ids, seen_sig = set(), set()
    for s_ in sc:
        sid = s_.get("id")
        if not sid or sid in seen_ids:
            errs.append(f"duplicate or missing scenario id {sid!r}")
        seen_ids.add(sid)
        if not isinstance(s_.get("page"), int) or s_["page"] <= 0:
            errs.append(f"{sid}: no page reference")
        scores = s_.get("scores") or {}
        unknown = [c for c in scores if c not in candidates]
        if unknown:
            errs.append(f"{sid}: unknown candidate ids {unknown}")
        if any(not isinstance(v, (int, float)) or v < 0 or v > 100 for v in scores.values()):
            errs.append(f"{sid}: a score outside 0..100")
        total = sum(v for v in scores.values() if isinstance(v, (int, float)))
        if abs(total - 100) > 2:
            errs.append(f"{sid}: scores sum to {total:g}, not 100 ± 2")
        if round_index == 1 and len(scores) != 2:
            errs.append(f"{sid}: a runoff duel needs exactly 2 candidates, got {len(scores)}")
        if round_index == 0 and len(scores) < 5:
            errs.append(f"{sid}: only {len(scores)} candidates")
        sig = tuple(sorted(scores.items()))
        if sig in seen_sig:
            errs.append(f"{sid}: same scores as another hypothesis of this poll")
        seen_sig.add(sig)
    return errs


def details_problems(details: dict, poll: dict, tol: float = 0.51) -> tuple[list[str], list[str]]:
    """(errors, warnings) for the details of one poll."""
    errs, warns = [], []
    scen = {s["id"]: s for s in poll.get("scenarios", [])}
    for ct in details.get("crosstabs", []):
        tag = f"p.{ct.get('page')} {ct.get('question')}"
        errs += [f"{tag}: {x}" for x in ct.get("problems", [])]
        sid = ct.get("scenario")
        if sid in scen and ct.get("total"):
            for c, v in scen[sid]["scores"].items():
                if c in ct["total"] and abs(ct["total"][c] - v) > tol:
                    errs.append(f"{tag}: Total {c}={ct['total'][c]} differs from the hypothesis ({v})")
        # The published score is a weighted mean of the men's and the women's: it lies between them (rounding aside).
        # This catches a cross-tab whose columns were read in the wrong order.
        sex = [g for g in ct.get("groups", []) if g.get("dimension") == "sexe" and g.get("category") in ("homme", "femme")]
        if sid in scen and len(sex) == 2 and ct.get("question", "").startswith("vote_intention"):
            for c, v in scen[sid]["scores"].items():
                a, b = sex[0]["values"].get(c), sex[1]["values"].get(c)
                if a is not None and b is not None and not min(a, b) - 1.6 <= v <= max(a, b) + 1.6:
                    errs.append(f"{tag}: {c} published {v} is outside the men/women range {min(a, b)}-{max(a, b)}")
        for g in ct.get("groups", []):
            for c, v in g["values"].items():
                if not 0 <= v <= 100:
                    errs.append(f"{tag}: {g['label']} {c}={v} outside 0..100")
            if not g.get("known", True):
                warns.append(f"{tag}: label {g['label']!r} not in the vocabulary")
    for c in details.get("certainty", []):
        for r in c["candidates"]:
            if abs(r["sure"] + r["may_change"] - 100) > 1:
                errs.append(f"certainty p.{c['page']}: {r['candidate']} sure+change = {r['sure'] + r['may_change']}")
        o = c.get("overall")
        if o and abs(o["sure"] + o["may_change"] - 100) > 1:
            errs.append(f"certainty p.{c['page']}: overall does not sum to 100")
    for q in details.get("camp_questions", []):
        for pop in q["populations"]:
            for a in pop["answers"]:
                if a.get("first") is not None and a.get("total") is not None and a["first"] > a["total"]:
                    errs.append(f"camp p.{q['page']}: {a['person']} first {a['first']} > total {a['total']}")
    return errs, warns
