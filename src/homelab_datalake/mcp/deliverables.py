"""Turning an answer into something a person can send to someone else.

This is the half of the MCP layer that is not a chatbot reply. A model that says
"you spent [spend withheld] in August" is a nice trick; a formatted workbook with the figures,
the period, and the query that produced them is a deliverable.

Two decisions worth defending:

**Every workbook carries its own provenance.** A second sheet records the dataset,
the range, the grain, the filters and the generated SQL. A spreadsheet outlives the
conversation that produced it, and one that cannot say what question it answers is
the reason people distrust generated reports. This one can be checked.

**An empty result is not exported.** A workbook with headers and no rows reads to a
human as "the answer is zero", which is a different claim from "no data matched".
Rather than produce a file that misleads, the export refuses and says which range
does hold data.

**The bytes come back, not only the path.** A workbook on this host is not a
deliverable for someone reading a chat window somewhere else; it is a file they
cannot reach. So `write_workbook` returns what it wrote alongside where it wrote it,
and the MCP layer hands those bytes to the client as an embedded resource. The file
on disk stays, because a local stdio client (and anyone reading the export directory)
still wants it, and because it is the only answer left for an export too large to
travel.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

EXPORT_DIR = os.environ.get("DATALAKE_EXPORT_DIR", "exports")
MAX_NAME_LENGTH = 60

_SAFE_NAME = re.compile(r"[^A-Za-z0-9_-]+")

_HEADER_FILL = PatternFill("solid", fgColor="1F2937")
_HEADER_FONT = Font(color="FFFFFF", bold=True)


@dataclass
class Deliverable:
    path: str
    filename: str
    rows: int
    bytes_written: int
    #: What was written. Held so the caller can deliver the file itself rather
    #: than a path to it -- see the module docstring.
    data: bytes


class ExportError(ValueError):
    pass


def safe_filename(name: str, suffix: str = ".xlsx") -> str:
    """A filename from a model is input, so it is rebuilt rather than checked.

    Everything outside a conservative alphabet is replaced, which disposes of path
    separators, `..`, NUL and Windows reserved characters in one step -- there is no
    pattern to have got wrong, because the output is constructed from an allow-list.
    """
    stem = _SAFE_NAME.sub("-", str(name or "")).strip("-")[:MAX_NAME_LENGTH]
    if not stem:
        stem = "export"
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return f"{stem}-{stamp}{suffix}"


def _autosize(sheet, widths: dict[int, int]) -> None:
    for index, width in widths.items():
        sheet.column_dimensions[get_column_letter(index)].width = min(max(width + 2, 10), 52)


def write_workbook(result, title: str, directory: str = EXPORT_DIR) -> Deliverable:
    """Write one query result as a formatted .xlsx, with a provenance sheet."""
    if result.row_count == 0:
        raise ExportError(
            "refusing to export an empty result: a workbook with headers and no rows "
            "reads as 'zero', which is a different claim from 'no data matched'. "
            + " ".join(result.notes)
        )

    book = Workbook()
    sheet = book.active
    sheet.title = "Data"

    columns = result.columns
    widths = {i: len(str(c)) for i, c in enumerate(columns, start=1)}

    sheet.append(columns)
    for index in range(1, len(columns) + 1):
        cell = sheet.cell(row=1, column=index)
        cell.fill, cell.font = _HEADER_FILL, _HEADER_FONT
        cell.alignment = Alignment(vertical="center")

    for row in result.rows:
        values = [row.get(c) for c in columns]
        sheet.append(values)
        for index, value in enumerate(values, start=1):
            widths[index] = max(widths[index], len(str(value)) if value is not None else 4)

    # Freeze the header, and let the reader filter -- the two things anyone does to a
    # table of figures within five seconds of opening it.
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    _autosize(sheet, widths)

    about = book.create_sheet("About this export")
    facts = [
        ("Question", title),
        ("Dataset", result.dataset),
        ("Range", f"{result.start} to {result.end} (inclusive)"),
        ("Grain", result.grain),
        ("Rows", result.row_count),
        ("Truncated", "yes -- this is not the whole answer" if result.truncated else "no"),
        ("Generated at", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")),
        ("Source", "homelab-datalake, https://data.example.test"),
        ("Query", result.sql),
    ]
    for note in result.notes:
        facts.append(("Note", note))

    for label, value in facts:
        about.append([label, str(value)])
    for index in range(1, len(facts) + 1):
        about.cell(row=index, column=1).font = Font(bold=True)
    about.column_dimensions["A"].width = 16
    about.column_dimensions["B"].width = 110
    for index in range(1, len(facts) + 1):
        about.cell(row=index, column=2).alignment = Alignment(wrap_text=True, vertical="top")

    target_dir = Path(directory)
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = safe_filename(title or result.dataset)
    path = (target_dir / filename).resolve()
    # Belt and braces: the name is constructed, but assert it landed where intended.
    if target_dir.resolve() not in path.parents:
        raise ExportError(f"refusing to write outside {target_dir.resolve()}")
    book.save(path)
    data = path.read_bytes()

    return Deliverable(
        path=str(path),
        filename=filename,
        rows=result.row_count,
        bytes_written=len(data),
        data=data,
    )
