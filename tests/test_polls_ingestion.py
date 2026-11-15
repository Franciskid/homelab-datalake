"""Poll ingestion: the vendored parsers read real-report fixtures, and the daily job publishes what passes and reviews the rest."""

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from homelab_datalake.assets import polls_ingestion as asset
from homelab_datalake.parsing.polls import discover, elabe, ingest, pipeline, validate
from homelab_datalake.parsing.polls import tables as pt

FIX = Path(__file__).parent / "fixtures" / "polls"
NAMES = {
    "nathalie-arthaud": "Nathalie Arthaud", "jean-luc-melenchon": "Jean-Luc Mélenchon", "fabien-roussel": "Fabien Roussel",
    "marine-tondelier": "Marine Tondelier", "raphael-glucksmann": "Raphaël Glucksmann", "edouard-philippe": "Édouard Philippe",
    "bruno-retailleau": "Bruno Retailleau", "david-lisnard": "David Lisnard", "nicolas-dupont-aignan": "Nicolas Dupont-Aignan",
    "marine-le-pen": "Marine Le Pen", "eric-zemmour": "Éric Zemmour", "francois-ruffin": "François Ruffin", "teddy-riner": "Teddy Riner",
    "gabriel-attal": "Gabriel Attal", "dominique-de-villepin": "Dominique de Villepin", "natacha-polony": "Natacha Polony",
}
needs_poppler = pytest.mark.skipif(shutil.which("pdftotext") is None, reason="poppler-utils (pdftotext) is not installed")


def minimal_pdf(pages):
    """A tiny PDF (Helvetica) from [[(x, y_from_top, text)]]: enough for pdftotext to give positions."""
    objs = [b"<< /Type /Catalog /Pages 2 0 R >>", b"", b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"]
    kids = []
    for items in pages:
        ops = [b"BT /F1 10 Tf %.1f %.1f Td (" % (x, 792 - y) + t.encode("cp1252", "replace").replace(b"(", b"\\(").replace(b")", b"\\)") + b") Tj ET"
               for x, y, t in items]
        stream = b"\n".join(ops)
        objs.append(b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream")
        objs.append(b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents %d 0 R /Resources << /Font << /F1 3 0 R >> >> >>" % (len(objs)))
        kids.append(len(objs))
    objs[1] = b"<< /Type /Pages /Kids [" + b" ".join(b"%d 0 R" % k for k in kids) + b"] /Count %d >>" % len(kids)
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objs, 1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % i + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1) + b"".join(b"%010d 00000 n \n" % o for o in offsets)
    out += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, xref)
    return bytes(out)


SCORES = [1, 15, 2, 3, 12, 16, 5, 6, 3, 33, 4]
TABLE = list(NAMES.values())[:11]


def report(scores=SCORES, dates="du 3 au 5 novembre 2026", institute="Ifop"):
    p1 = [(50, 60, f"Sondage {institute}. Enquête réalisée {dates}."), (50, 80, "Échantillon de 1 500 personnes inscrites."),
          (50, 100, "Intentions de vote à l'élection présidentielle, premier tour.")]
    p2 = [(50, 60, "L'intention de vote au premier tour de l'élection présidentielle"), (400, 100, "Résultats publiés (%)")]
    for i, name in enumerate(TABLE):
        p2 += [(50, 130 + 18 * i, name), (450, 130 + 18 * i, str(scores[i]))]
    return minimal_pdf([p1, p2])


LIST = ('<h2 class="notices-mois-titre" rel="pannel_0">Novembre 2026</h2><img src="x"/><div id=pannel_0 class="notices-elements">'
        '<p class="download-line"><a href="/notices/medias/fichiers/add/9001" class="pdf_download" target="_blank">10400 Pres IV IFOP Journal 6 novembre</a></p>'
        '<p class="download-line"><a href="/notices/medias/fichiers/add/9003" class="pdf_download" target="_blank">10402 Pres IV TOLUNA HARRIS INTERACTIVE 8 novembre</a></p>'
        '</div>')
BASE = "https://www.commission-des-sondages.fr"


class Fetcher:
    def __init__(self, files):
        self.files, self.calls = files, []

    def __call__(self, url):
        self.calls.append(url)
        if url not in self.files:
            raise OSError("unreachable: " + url)
        return self.files[url], ""


FILES = {BASE + "/notices/": LIST.encode(), BASE + "/notices/medias/fichiers/add/9001": report(),
         BASE + "/notices/medias/fichiers/add/9003": report(institute="Harris", dates="du 5 au 7 novembre 2026"),
         "https://politico.fraserv.fr/polls-index.json": json.dumps({"candidates": NAMES, "rounds": [[], []]}).encode()}
TODAY = datetime(2026, 11, 15, 6, 10, tzinfo=UTC)


class Log:
    def warning(self, *a):
        pass

    def info(self, *a):
        pass


# --- the vendored parsers on real reports -------------------------------------------------------

@needs_poppler
def test_elabe_recap_pages_of_the_14_november_report():
    scores, _, _ = elabe._recap(FIX / "elabe-recap-pages-8-22.pdf", 1, NAMES)
    assert scores[4]["david-lisnard"] == 10.5 and scores[4]["francois-ruffin"] == 3.0
    for s in scores.values():
        assert abs(sum(s.values()) - 100) < 0.01
    runoff, _, _ = elabe._recap(FIX / "elabe-recap-pages-8-22.pdf", 2, NAMES)
    assert runoff[1] == {"jean-luc-melenchon": 31.5, "marine-le-pen": 68.5}


def test_elabe_crosstab_certainty_and_camp_questions_from_text_pages():
    pages = (FIX / "elabe-2026-11-14-pages.txt").read_text(encoding="utf-8").split("\f")
    assert elabe.meta(pages)["sample"] == 1500
    cols = ["nathalie-arthaud", "fabien-roussel", "jean-luc-melenchon", "marine-tondelier", "raphael-glucksmann", "edouard-philippe",
            "dominique-de-villepin", "bruno-retailleau", "nicolas-dupont-aignan", "marine-le-pen", "eric-zemmour"]
    ct = elabe.crosstab_from_page(pages[31], cols, "vote_intention_t1", "s", 32)
    assert ct["problems"] == [] and len(ct["groups"]) == 30 and ct["total"]["marine-le-pen"] == 33.0
    by = {(g["dimension"], g["category"]): g for g in ct["groups"]}
    assert by[("age", "18-24")]["values"]["jean-luc-melenchon"] == 44.0 and by[("age", "18-24")]["label"] == "18-24 ans"
    cert = elabe.certainty(pages[10], NAMES, 11, "s", "base")
    assert cert["overall"] == {"sure": 55.0, "sure_delta": 1, "may_change": 45.0, "may_change_delta": -1}
    camp = elabe.camp_question(pages[29], 30)
    sym = {a["person"]: a for a in camp["populations"][0]["answers"]}
    assert (sym["D. Lisnard"]["first"], sym["D. Lisnard"]["total"]) == (27.0, 43.0)
    assert validate.details_problems({"certainty": [cert], "camp_questions": [camp]}, {"scenarios": []}) == ([], [])


def test_commission_list_and_wikipedia_rows():
    notices = discover.parse_commission_notices((FIX / "commission-notices.html").read_text(encoding="utf-8"))
    first = notices[0]
    assert (first["no"], first["institute"], first["published"], first["vote_intentions"]) == (10371, "OpinionWay", "2026-11-12", True)
    rows = {(r["institute"], r["end"]): r for r in discover.parse_wikipedia_polls((FIX / "wikipedia-rows.html").read_text(encoding="utf-8"))}
    assert rows[("Elabe", "2026-11-13")]["sample"] == 1500


def test_validation_and_duplicates():
    poll = {"id": "a", "institute": "X", "start": "2026-11-03", "end": "2026-11-05", "sample": 1500, "source": "https://e.test/a.pdf",
            "scenarios": [{"id": "s", "page": 2, "scores": {"marine-le-pen": 50, "edouard-philippe": 20, "david-lisnard": 10, "eric-zemmour": 10, "marine-tondelier": 5}}]}
    assert any("sum to" in e for e in validate.poll_problems(poll, 0, NAMES, date(2026, 11, 15)))
    poll["scenarios"][0]["scores"]["marine-le-pen"] = 55
    assert validate.poll_problems(poll, 0, NAMES, date(2026, 11, 15)) == []
    assert pipeline.is_duplicate(dict(poll, id="b"), [poll]) is not None
    assert pipeline.is_duplicate(dict(poll, id="c", institute="Y"), [poll]) is None


# --- the job ------------------------------------------------------------------------------------

@needs_poppler
def test_job_publishes_what_passes_and_reviews_the_rest(tmp_path):
    export = asset.run_job(Fetcher(FILES), tmp_path, TODAY, Log())
    published = [p["id"] for r in export["rounds"] for p in r["polls"]]
    assert published == ["ifop_2026-11-03_2026-11-05_1500"]
    poll = export["rounds"][0]["polls"][0]
    assert poll["reviewed"] is False and poll["ingest"]["mode"] == "auto"
    review = {r["poll_id"]: r for r in export["review"]}
    assert "harris_2026-11-05_2026-11-07_1500" in review                       # valid, but Harris's reader is not proven: review, not published
    assert export["job"]["ingested"] == 1 and export["job"]["review"] == 1
    assert all(not s["ok"] for s in export["job"]["sources"] if s["source"] == "wikipedia")  # unreachable here: reported, not fatal


@needs_poppler
def test_export_is_written_whole_and_the_next_run_remembers(tmp_path):
    export = asset.run_job(Fetcher(FILES), tmp_path, TODAY, Log())
    path = asset.write_export(tmp_path, export)
    assert path.name == "polls-export.json" and not (tmp_path / ".polls-export.json.tmp").exists()
    assert json.loads(path.read_text(encoding="utf-8"))["schema"] == 1
    again = Fetcher(FILES)
    second = asset.run_job(again, tmp_path, TODAY, Log())
    assert BASE + "/notices/medias/fichiers/add/9001" not in again.calls     # already read: not downloaded again
    assert [p["id"] for p in second["rounds"][0]["polls"]] == ["ifop_2026-11-03_2026-11-05_1500"]   # still published


@needs_poppler
def test_a_failing_poll_is_never_published(tmp_path):
    files = dict(FILES)
    files[BASE + "/notices/medias/fichiers/add/9001"] = report(dates="du 20 au 22 novembre 2026")   # fieldwork in the future
    export = asset.run_job(Fetcher(files), tmp_path, TODAY, Log())
    assert export["rounds"][0]["polls"] == []
    assert any("future" in reason for r in export["review"] for reason in r["reasons"])


def test_no_baseline_means_no_run(tmp_path):
    files = {k: v for k, v in FILES.items() if "polls-index" not in k}
    with pytest.raises(RuntimeError, match="baseline"):
        asset.run_job(Fetcher(files), tmp_path, TODAY, Log())


def test_a_site_that_cannot_be_reached_falls_back_to_the_seed(tmp_path):
    files = {k: v for k, v in FILES.items() if "polls-index" not in k}
    (tmp_path / "polls-index.json").write_text(json.dumps({"candidates": NAMES, "rounds": [[], []]}), encoding="utf-8")
    export = asset.run_job(Fetcher(files), tmp_path, TODAY, Log())
    assert export["schema"] == 1


def test_only_https_is_fetched():
    with pytest.raises(ValueError, match="non-https"):
        asset.http_fetch("http://example.test/a.pdf")


def test_schedule_is_daily_after_the_official_statistics():
    assert asset.polls_ingestion_schedule.cron_schedule == "10 6 * * *"
    assert asset.polls_ingestion_job.name == "polls_ingestion"


def test_the_vendored_copy_is_the_one_in_politico():
    # the parsers must stay what politico tests; this fails loudly if someone edits the copy here
    src = Path(pt.__file__).parent
    for f in ("elabe.py", "ingest.py", "validate.py"):
        assert (src / f).read_text(encoding="utf-8").startswith("# Vendored from the politico repository"), f
    assert pipeline.AUTO_PARSERS == {"elabe-full"} and ingest.SCHEMA == 1
