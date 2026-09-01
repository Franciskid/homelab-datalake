"""The tool surface, driven the way a model drives it.

These go through `call_tool` rather than calling the functions, because the schema
and the serialisation are part of what is being tested: a tool whose result cannot
be encoded is broken however correct the query was.
"""

import asyncio
import base64
import json
from io import BytesIO

import pytest
from openpyxl import load_workbook

from homelab_datalake import serving
from homelab_datalake.mcp import server as server_module
from tests.conftest import build_marts


@pytest.fixture
def tools(tmp_path, monkeypatch):
    """The server, pointed at a published serving copy."""
    warehouse = str(tmp_path / "datalake.duckdb")
    target = str(tmp_path / "serving.duckdb")
    build_marts(warehouse)
    serving.publish(warehouse, target)
    monkeypatch.setattr(server_module, "_warehouse", server_module.Warehouse(target))
    return server_module.server


def call(server, name, arguments=None):
    result = asyncio.run(server.call_tool(name, arguments or {}))
    return result.structured_content or json.loads(result.content[0].text)


def test_every_tool_is_registered(tools):
    names = {t.name for t in asyncio.run(tools.list_tools())}
    assert names == {
        "list_datasets", "describe_dataset", "describe_schema", "llm_spend",
        "web_traffic", "camera_activity", "media_library", "export_workbook",
    }


def test_every_tool_has_a_description(tools):
    """It is the only thing the model reads when deciding whether to call it."""
    for tool in asyncio.run(tools.list_tools()):
        assert tool.description and len(tool.description) > 40, tool.name


def test_there_is_no_tool_that_takes_sql(tools):
    """The design in one assertion: no parameter anywhere accepts a query."""
    for tool in asyncio.run(tools.list_tools()):
        properties = (tool.input_schema or {}).get("properties", {})
        assert "sql" not in properties and "query" not in properties, tool.name


def test_listing_datasets_reports_what_each_one_covers(tools):
    payload = call(tools, "list_datasets")
    covered = {d["name"]: d["covers"] for d in payload["datasets"]}
    assert covered["llm_spend"] == {"from": "2026-08-01", "to": "2026-08-20"}
    assert "total" in payload["grains"]


def test_describe_lists_the_values_a_dimension_takes(tools):
    """Without this the model guesses, and a guessed filter matches nothing."""
    payload = call(tools, "describe_dataset", {"dataset": "camera_activity"})
    values = {d["name"]: d["values"] for d in payload["dimensions"]}
    assert set(values["lifecycle"]) == {"candidate", "confirmed", "dismissed"}
    # Hours must come back in numeric order, or the busiest-hour answer is nonsense.
    assert values["started_hour_of_day"] == list(range(24))


def test_a_high_cardinality_dimension_is_summarised_not_dumped(tools):
    """A column with hundreds of values would fill the context with a list nobody reads."""
    payload = call(tools, "describe_dataset", {"dataset": "media_library"})
    values = {d["name"]: d["values"] for d in payload["dimensions"]}
    # 30 production years in the fixture, above the 25 the tool will enumerate.
    assert isinstance(values["production_year"], str)
    assert "many distinct values" in values["production_year"]
    # A small one is still listed in full, which is what makes filtering possible.
    assert set(values["resolution_class"]) == {"4K", "1080p", "720p"}


def test_a_question_returns_rows_and_the_sql_that_produced_them(tools):
    payload = call(tools, "llm_spend", {
        "start": "2026-08-01", "end": "2026-08-20", "grain": "total",
        "dimensions": ["provider"], "measures": ["calls", "spend_usd"],
    })
    assert payload["row_count"] == 3
    assert "SELECT" in payload["sql"]


def test_a_bad_call_returns_the_reason_rather_than_raising(tools):
    """The model fixes its own call from this message; a stack trace teaches it nothing."""
    payload = call(tools, "llm_spend", {
        "start": "2026-08-01", "end": "2026-08-20", "measures": ["revenue"],
    })
    assert "revenue" in payload["error"]
    assert "Available" in payload["error"]
    assert payload["row_count"] == 0


def test_an_unknown_dataset_names_the_real_ones(tools):
    payload = call(tools, "describe_dataset", {"dataset": "spending"})
    assert "llm_spend" in payload["error"]


def test_exporting_writes_a_workbook(tools, tmp_path, monkeypatch):
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    payload = call(tools, "export_workbook", {
        "dataset": "llm_spend", "title": "spend by model",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "total",
        "dimensions": ["model"], "measures": ["calls", "spend_usd"],
    })
    assert payload["rows"] == 3
    assert payload["filename"].startswith("spend-by-model-")
    assert payload["bytes"] > 0


def test_exporting_nothing_returns_an_error_not_a_file(tools):
    payload = call(tools, "export_workbook", {
        "dataset": "camera_activity", "title": "quiet week",
        "start": "2026-01-01", "end": "2026-01-05",
    })
    assert "empty result" in payload["error"]
    assert payload["row_count"] == 0


def test_describe_schema_hands_over_the_join_graph(tools):
    """The tool that exists so an agent stops guessing joins and retrying."""
    out = call(tools, "describe_schema")
    assert "tables" in out and "edges" in out
    # Shape, not contents: the manifest is built at image build time, so a checkout
    # without one must still get a usable answer rather than an error.
    assert isinstance(out["tables"], list)
    assert isinstance(out["edges"], list)


def test_the_workbook_itself_comes_back_with_the_answer(tools, tmp_path, monkeypatch):
    """The point of the whole tool: the file travels, not a path to it.

    A path on the datalake host is not a deliverable to somebody reading a chat on
    another machine, and when a path was all this returned, two agent runs duly
    reported a workbook as attached to a conversation that had no attachment in it.
    The bytes now ride back as an MCP embedded resource, which is the standard shape
    for exactly this and needs nothing datalake-specific at the other end.
    """
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    result = asyncio.run(tools.call_tool("export_workbook", {
        "dataset": "camera_activity", "title": "Portal events",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
    }))

    resources = [b for b in result.content if b.type == "resource"]
    assert len(resources) == 1, [b.type for b in result.content]
    blob = resources[0].resource

    summary = json.loads(result.content[0].text)
    # A host saves the file under the last segment of the URI, so that segment has to
    # be the name -- not a directory, and not the whole path.
    assert str(blob.uri).endswith(f"/{summary['filename']}")
    assert blob.mime_type == server_module.XLSX_MIME

    written = base64.b64decode(blob.blob)
    assert len(written) == summary["bytes"]
    # It is the workbook, not a description of one.
    assert load_workbook(BytesIO(written)).sheetnames == ["Data", "About this export"]


def test_the_bytes_are_not_shown_to_the_model(tools, tmp_path, monkeypatch):
    """A megabyte of base64 in a text block would cost more than the answer is worth.

    A host builds the model's view of a tool result out of its text blocks. Keeping
    the file in a resource block rather than encoding it into the summary is what
    makes attaching one free, in context terms, however large it is.
    """
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    result = asyncio.run(tools.call_tool("export_workbook", {
        "dataset": "camera_activity", "title": "Portal events",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
    }))
    text = "\n".join(b.text for b in result.content if b.type == "text")
    assert len(text) < 2_000
    assert "blob" not in text


def test_an_export_that_says_it_is_attached_has_something_attached(tools, tmp_path,
                                                                   monkeypatch):
    """The sentence the model repeats, and the block that makes it true, in one test.

    These two must not be able to drift apart: a `delivery` that claims an attachment
    with no resource block beside it is precisely the failure this tool already had
    once, told the other way round.
    """
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    result = asyncio.run(tools.call_tool("export_workbook", {
        "dataset": "camera_activity", "title": "Portal events",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
    }))
    delivery = json.loads(result.content[0].text)["delivery"]
    assert "attached" in delivery and "NOT attached" not in delivery
    assert any(b.type == "resource" for b in result.content)


def test_an_export_too_large_to_travel_says_where_it_went_instead(tools, tmp_path,
                                                                  monkeypatch):
    """The one case where a path is the honest answer -- and it is labelled as such."""
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr(server_module, "ATTACHABLE_BYTES", 16)
    result = asyncio.run(tools.call_tool("export_workbook", {
        "dataset": "camera_activity", "title": "Portal events",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
    }))
    summary = json.loads(result.content[0].text)
    assert not [b for b in result.content if b.type == "resource"]
    assert "NOT attached" in summary["delivery"]
    assert summary["path"] in summary["delivery"]


def test_a_boolean_dimension_can_be_filtered_with_a_boolean(tools):
    """The schema must accept the values describe_dataset advertises.

    `state_changed` is reported as `[false, true]`. Declaring filters as
    `dict[str, list[str]]` meant filtering on `true` was refused before it reached
    the query layer -- "Input validation error: True is not of type 'string'". An
    agent that hit that could not tell what was wrong, concluded its range was too
    wide, and split one correct query into thirty until it ran out of steps.
    """
    booleans = call(tools, "camera_activity", {
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
        "fields": ["happened_at", "zone_state"],
        "filters": {"state_changed": [True]},
    })
    assert "error" not in booleans, booleans
    assert booleans["row_count"] > 0

    # The string form keeps working, because DuckDB coerces it and because a caller
    # that learned the old shape should not break.
    strings = call(tools, "camera_activity", {
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
        "fields": ["happened_at", "zone_state"],
        "filters": {"state_changed": ["true"]},
    })
    assert strings["row_count"] == booleans["row_count"]


def test_a_numeric_dimension_can_be_filtered_with_a_number(tools):
    payload = call(tools, "camera_activity", {
        "start": "2026-08-01", "end": "2026-08-20", "grain": "total",
        "dimensions": ["started_hour_of_day"], "measures": ["events"],
        "filters": {"started_hour_of_day": [7, 8]},
    })
    assert "error" not in payload, payload
    assert {row["started_hour_of_day"] for row in payload["rows"]} <= {7, 8}
