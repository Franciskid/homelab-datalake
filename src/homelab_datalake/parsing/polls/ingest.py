# Vendored from the politico repository (scripts/pollslib/ingest.py, 282ab13). Do not edit here: change it there and run scripts/vendor-pollslib.py.
"""One run of the daily poll ingestion: discover, download, extract, validate, classify, export.

The network is injected (`fetch(url) -> (bytes, content_type)`), so the same function runs on fixtures in tests, on live pages
in the datalake job and in `scripts/polls-ingest.py dry-run`. It never publishes a poll that fails a check: those go to
`review`, with the reasons, and stay there until someone accepts them (scripts/polls-review.py).

The export it returns holds only what is new compared to the site's bundled data (`baseline`): the site merges it at run
time (src/observatory/live-polls.js) and the bundled copy wins on any conflict.
"""
import hashlib
import json
import re
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import discover, pipeline, validate

SCHEMA = 1
MAX_PDF_BYTES = 60_000_000
MAX_ARTICLES_PER_SOURCE = 8
MAX_DOCUMENTS = 60
USER_AGENT = "fraserv-polls/1.0 (+https://politico.fraserv.fr)"
FINAL = {"ingested", "review", "duplicate", "not-a-poll", "skipped-old"}


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def is_pdf(data: bytes, content_type: str = "") -> bool:
    return data[:5] == b"%PDF-"


def looks_like_vote_report(text: str) -> bool:
    t = discover._norm(text)
    return "intention" in t and bool(re.search(r"1er tour|premier tour|1e tour", t)) and bool(re.search(r"presidentielle", t))


def has_candidate_table(text: str, names: dict) -> bool:
    """At least 6 candidate names and a page of at least 10 figures: a layout the table reader should have understood."""
    from . import tables as pt
    alias = pt.aliases(names)
    for page in text.split("\f"):
        n = pt.norm(page)
        if len({cid for a, cid in alias if len(a) > 6 and a in n}) >= 6 and len(re.findall(r"(?<![\w.,])\d{1,3}(?:[.,]\d+)?%?(?!\w)", page)) >= 10:
            return True
    return False


def index_of(rounds: list[dict]) -> dict:
    """Compact picture of the polls the site already has: enough to de-duplicate and to know the candidate ids."""
    return {"candidates": rounds[0].get("candidates", {}),
            "rounds": [[{"id": p["id"], "institute": p["institute"], "start": p["start"], "end": p["end"], "source": p.get("source"),
                         "scenarios": [{"scores": s["scores"]} for s in p["scenarios"][:3]]} for p in r["polls"]] for r in rounds]}


def discover_candidates(fetch, today: date, since_days: int, previous_urls: set[str], sources=None, log=print) -> tuple[list[dict], list[dict], list[dict]]:
    """(documents to try, source reports, wikipedia rows)."""
    cands: list[dict] = []
    seen_urls: set[str] = set()
    sources_report: list[dict] = []
    cutoff = today - timedelta(days=since_days)

    def add(c):
        if c["url"] not in seen_urls and c["url"] not in previous_urls:
            seen_urls.add(c["url"])
            cands.append(c)

    # 1. the Commission's list: every notice of voting intentions published in the window
    try:
        data, _ = fetch("https://www.commission-des-sondages.fr/notices/")
        notices = discover.parse_commission_notices(data.decode("utf-8", "replace"))
        recent = [n for n in notices if n["vote_intentions"] and date.fromisoformat(n["published"]) >= cutoff]
        for n in recent:
            add({"url": n["url"], "institute": n["institute"], "via": "commission", "published": n["published"], "title": n["title"]})
        sources_report.append({"source": "commission-des-sondages", "ok": True, "found": len(recent), "listed": len(notices)})
    except Exception as e:  # noqa: BLE001 -- a source that fails must not stop the others
        sources_report.append({"source": "commission-des-sondages", "ok": False, "error": str(e)[:200]})
    # 2. the institutes' own pages: listing -> article -> PDF
    for src in sources or discover.SOURCES:
        found, err = 0, None
        try:
            for listing in src["listing"]:
                page, _ = fetch(listing)
                page = page.decode("utf-8", "replace")
                direct = discover.pdf_links(page, listing)
                arts = [u for u in discover.article_links(page, listing, src["article"]) if u not in direct][:MAX_ARTICLES_PER_SOURCE]
                for u in direct:
                    if discover.REPORT_URL.search(u):
                        add({"url": u, "institute": src["institute"], "via": "institute-page", "page": listing})
                        found += 1
                for art in arts:
                    if art in previous_urls:
                        continue
                    try:
                        html, _ = fetch(art)
                        for u in discover.pdf_links(html.decode("utf-8", "replace"), art):
                            if not discover.REPORT_URL.search(u):
                                continue
                            add({"url": u, "institute": src["institute"], "via": "institute-page", "page": art})
                            found += 1
                    except Exception as e:  # noqa: BLE001
                        err = f"{art}: {e}"[:200]
        except Exception as e:  # noqa: BLE001
            err = str(e)[:200]
        sources_report.append({"source": src["institute"], "ok": err is None or found > 0, "found": found, **({"error": err} if err else {})})
    # 3. Wikipedia: a signal only (it has no primary document), compared with what we have afterwards
    wiki = []
    try:
        data, _ = fetch(discover.WIKI_URL)
        wiki = [w for w in discover.parse_wikipedia_polls(data.decode("utf-8", "replace")) if date.fromisoformat(w["end"]) >= cutoff]
        sources_report.append({"source": "wikipedia", "ok": True, "found": len(wiki)})
    except Exception as e:  # noqa: BLE001
        sources_report.append({"source": "wikipedia", "ok": False, "error": str(e)[:200]})
    return cands[:MAX_DOCUMENTS], sources_report, wiki


def process_document(cand: dict, data: bytes, names: dict, known: list[list[dict]], today: date, since_days: int, workdir: Path | None = None) -> dict:
    """Parse one downloaded PDF. Returns {'status','reasons','polls':[{round, poll, status, details, duplicate_of}], 'sha256', ...}."""
    out = {"url": cand["url"], "institute": cand.get("institute"), "via": cand.get("via"), "sha256": sha256(data), "polls": [], "reasons": []}
    with tempfile.TemporaryDirectory(prefix="poll-pdf-") as tmp:
        path = Path(tmp) / "doc.pdf"
        path.write_bytes(data)
        try:
            text = pipeline.pdf_text(path)
        except Exception as e:  # noqa: BLE001
            return {**out, "status": "review", "reasons": [f"pdftotext failed: {str(e)[:120]}"]}
        if not looks_like_vote_report(text):
            return {**out, "status": "not-a-poll", "reasons": ["no first-round voting intentions in the text"]}
        inst = cand.get("institute") or discover.institute_of(text[:3000]) or discover.institute_of(cand["url"])
        if not inst:
            return {**out, "status": "review", "reasons": ["institute not identified"]}
        out["institute"] = inst
        try:
            parsed = pipeline.parse_document(path, text, inst, cand["url"], names, hint={"published": cand.get("published")})
        except Exception as e:  # noqa: BLE001 -- a parser bug becomes a review item, never a crash
            return {**out, "status": "review", "reasons": [f"parser error: {str(e)[:160]}"]}
    out["parser"] = parsed["parser"]
    cutoff = today - timedelta(days=since_days)
    if not parsed["polls"]:
        if not has_candidate_table(text, names):
            return {**out, "status": "not-a-poll", "reasons": ["no table of candidates with figures"]}
        return {**out, "status": "review", "reasons": parsed["problems"] or ["a table of candidates was found but no hypothesis passed the checks (sum 100 ± 2, at least 5 candidates, known ids)"]}
    for ri, poll in parsed["polls"]:
        dup = pipeline.is_duplicate(poll, known[ri])
        entry = {"round": ri, "poll": poll, "duplicate_of": dup["id"] if dup else None}
        if dup:
            entry["status"] = "duplicate"
        else:
            d = pipeline.details_for(parsed["details"], poll) if parsed["details"] else None
            derrs, dwarn = validate.details_problems(d or {}, poll)
            verdict, reasons = pipeline.classify(parsed, ri, poll, names, derrs, today)
            if poll.get("end") and poll["end"] < cutoff.isoformat():
                verdict, reasons = "review", reasons + [f"fieldwork ended {poll['end']}, before the {since_days}-day window"]
            entry.update({"status": "ingested" if verdict == "auto" else "review", "reasons": reasons, "warnings": dwarn[:5], "details": d})
            known[ri].append(poll)  # a second document of the same poll in this run is a duplicate
        out["polls"].append(entry)
    states = {e["status"] for e in out["polls"]}
    out["status"] = "ingested" if "ingested" in states else "review" if "review" in states else "duplicate"
    return out


def run(fetch, baseline: dict, previous: dict | None, today: date, since_days: int = 30, sources=None, now: datetime | None = None,
        log=print, replay_from: date | None = None) -> dict:
    """Returns the export (dict). `baseline` is index_of(...) of the site's data; `previous` the last export, if any.
    `replay_from` ignores baseline polls that ended on or after that date: a dry run that shows what the job would have done."""
    now = now or datetime.now(timezone.utc)
    previous = previous or {}
    names = dict(baseline["candidates"])
    known = [list(r) for r in baseline["rounds"]]
    if replay_from:
        known = [[p for p in r if p["end"] < replay_from.isoformat()] for r in known]
    prev_docs = {d["url"]: d for d in previous.get("documents", [])}
    prev_polls = [list(r.get("polls", [])) for r in previous.get("rounds", [{}, {}])]
    for ri in (0, 1):
        known[ri] += [p for p in (prev_polls[ri] if ri < len(prev_polls) else []) if p["id"] not in {k["id"] for k in known[ri]}]
    previous_urls = {u for u, d in prev_docs.items() if d.get("status") in FINAL}
    cands, sources_report, wiki = discover_candidates(fetch, today, since_days, previous_urls, sources, log)
    documents = {u: d for u, d in prev_docs.items()}
    new_rounds = [list(p) for p in prev_polls] + [[] for _ in range(2 - len(prev_polls))]
    new_details = {"polls": dict(previous.get("details", {}).get("polls", {})), "people": dict(previous.get("details", {}).get("people", {}))}
    review = {r["poll_id"]: r for r in previous.get("review", [])}
    baseline_ids = {p["id"] for r in known for p in r}
    failed, seen_sha = [], {d.get("sha256"): u for u, d in prev_docs.items() if d.get("sha256")}
    counts = {"discovered": len(cands), "downloaded": 0, "ingested": 0, "review": 0, "duplicates": 0, "not_polls": 0}
    for cand in cands:
        try:
            data, ctype = fetch(cand["url"])
            if len(data) > MAX_PDF_BYTES or not is_pdf(data, ctype):
                documents[cand["url"]] = {"url": cand["url"], "status": "not-a-poll", "reasons": ["not a PDF or too large"], "at": now.isoformat(timespec="seconds")}
                counts["not_polls"] += 1
                continue
        except Exception as e:  # noqa: BLE001
            failed.append({"url": cand["url"], "error": str(e)[:160]})
            continue
        counts["downloaded"] += 1
        h = sha256(data)
        if h in seen_sha:
            documents[cand["url"]] = {"url": cand["url"], "status": "duplicate", "sha256": h, "reasons": [f"same file as {seen_sha[h]}"], "at": now.isoformat(timespec="seconds")}
            counts["duplicates"] += 1
            continue
        seen_sha[h] = cand["url"]
        try:
            res = process_document(cand, data, names, known, today, since_days)
        except Exception as e:  # noqa: BLE001 -- one bad document must not stop the run
            res = {"status": "review", "reasons": [f"unexpected error: {str(e)[:160]}"], "polls": [], "institute": cand.get("institute")}
        ids = []
        for e in res["polls"]:
            poll = e["poll"]
            if e["status"] == "duplicate":
                counts["duplicates"] += 1
            elif e["status"] == "ingested":
                counts["ingested"] += 1
                stamped = pipeline.stamp(poll, "auto", res["parser"], "auto", [], now)
                stamped["document"] = h[:12]
                new_rounds[e["round"]].append(stamped)
                if e["details"]:
                    d = {k: v for k, v in e["details"].items() if k != "people" and v}
                    if d:
                        new_details["polls"][poll["id"]] = d
                    new_details["people"].update(e["details"].get("people") or {})
                ids.append(poll["id"])
            else:
                counts["review"] += 1
                review[poll["id"]] = {"poll_id": poll["id"], "round": e["round"], "institute": poll["institute"], "end": poll["end"], "source": poll["source"],
                                      "status": "review", "reasons": e["reasons"], "poll": pipeline.stamp(poll, "auto", res["parser"], "review", e["reasons"], now),
                                      "details": e.get("details"), "found_at": now.isoformat(timespec="seconds")}
                ids.append(poll["id"])
        if res["status"] == "not-a-poll":
            counts["not_polls"] += 1
        if res["status"] == "review" and not res["polls"]:
            counts["review"] += 1
            review[f"doc:{h[:12]}"] = {"poll_id": f"doc:{h[:12]}", "round": None, "institute": res.get("institute"), "end": None, "source": cand["url"], "status": "review",
                                       "reasons": res["reasons"], "found_at": now.isoformat(timespec="seconds")}
        documents[cand["url"]] = {"url": cand["url"], "status": res["status"], "institute": res.get("institute"), "sha256": h, "via": cand.get("via"), "parser": res.get("parser"),
                                  "polls": ids, "reasons": res["reasons"], "at": now.isoformat(timespec="seconds")}
    # a review item whose poll the site now has (after a deploy) or that a later run ingested is no longer pending
    review = {k: v for k, v in review.items() if k not in baseline_ids and k not in {p["id"] for r in new_rounds for p in r}}
    # Wikipedia rows with no poll of ours on those dates: a hint that a report is missing
    allp = [p for r in known + new_rounds for p in r]
    wiki_only = [w for w in wiki if not any(p["institute"] == w["institute"] and abs((date.fromisoformat(p["end"]) - date.fromisoformat(w["end"])).days) <= 1 for p in allp)]
    return {"schema": SCHEMA, "generated_at": now.isoformat(timespec="seconds"),
            "rounds": [{"candidates": names, "polls": sorted(new_rounds[0], key=lambda p: (p["end"], p["id"]))},
                       {"candidates": names, "polls": sorted(new_rounds[1], key=lambda p: (p["end"], p["id"]))}],
            "details": {"version": pipeline.DETAILS_VERSION, **new_details},
            "review": sorted(review.values(), key=lambda r: (r.get("end") or "", r["poll_id"])),
            "documents": sorted(documents.values(), key=lambda d: d.get("at", "")),
            "job": {**counts, "since_days": since_days, "sources": sources_report, "failed": failed, "wikipedia_only": wiki_only}}
