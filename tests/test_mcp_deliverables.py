"""The workbook: that it is safe to write, and honest about what it contains."""

from pathlib import Path

import pytest
from openpyxl import load_workbook

from homelab_datalake.mcp.deliverables import ExportError, safe_filename, write_workbook
from homelab_datalake.mcp.query import run_query

AUGUST = {"start": "2026-08-01", "end": "2026-08-20"}


@pytest.mark.parametrize(
    "hostile",
    [
        "../../etc/passwd",
        "/etc/passwd",
        "..\\..\\windows\\system32",
        "spend\x00.xlsx",
        "con",  # a Windows reserved name
        "a" * 500,
    ],
)
def test_filenames_are_rebuilt_from_an_allow_list(hostile):
    """Not validated -- constructed. There is no pattern here to have got wrong."""
    name = safe_filename(hostile)
    assert "/" not in name and "\\" not in name
    assert "\x00" not in name
    assert ".." not in name
    assert name.endswith(".xlsx")
    assert len(name) < 100


def test_an_empty_name_still_produces_a_file():
    assert safe_filename("").startswith("export-")
    assert safe_filename("!!!").startswith("export-")


def test_the_workbook_carries_the_data_and_its_provenance(con, tmp_path):
    result = run_query(con, "llm_spend", grain="total", dimensions=["provider"],
                       measures=["calls", "spend_usd"], **AUGUST)
    written = write_workbook(result, "August spend by provider", directory=str(tmp_path))

    assert written.rows == 3
    book = load_workbook(written.path)
    assert book.sheetnames == ["Data", "About this export"]

    rows = list(book["Data"].iter_rows(values_only=True))
    assert rows[0] == ("provider", "calls", "spend_usd")
    assert len(rows) == 4  # header plus three providers
    assert book["Data"].freeze_panes == "A2"

    # The provenance sheet is the point: a spreadsheet outlives the conversation, and
    # one that cannot say what question it answers is why people distrust these.
    about = {row[0]: row[1] for row in book["About this export"].iter_rows(values_only=True)}
    assert about["Question"] == "August spend by provider"
    assert about["Dataset"] == "llm_spend"
    assert "2026-08-01 to 2026-08-20" in about["Range"]
    assert "SELECT" in about["Query"]


def test_an_empty_result_is_refused_rather_than_exported(con, tmp_path):
    """A sheet with headers and no rows reads to a person as 'the answer is zero'."""
    result = run_query(con, "camera_activity", start="2026-01-01", end="2026-01-05")
    with pytest.raises(ExportError, match="empty result"):
        write_workbook(result, "nothing", directory=str(tmp_path))
    assert not list(Path(tmp_path).glob("*.xlsx"))


def test_a_truncated_export_says_so_in_the_workbook(con, tmp_path):
    result = run_query(con, "web_traffic", grain="day",
                       dimensions=["status_class", "client_kind"],
                       measures=["requests"], limit=5, **AUGUST)
    written = write_workbook(result, "partial", directory=str(tmp_path))
    about = load_workbook(written.path)["About this export"]
    values = " ".join(str(c) for row in about.iter_rows(values_only=True) for c in row)
    assert "not the whole answer" in values
