"""The tool surface, driven the way a model drives it.

These go through `call_tool` rather than calling the functions, because the schema
and the serialisation are part of what is being tested: a tool whose result cannot
be encoded is broken however correct the query was.
"""

import asyncio
import json

import pytest

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


def test_an_export_says_it_is_not_attached_to_the_conversation(tools, tmp_path, monkeypatch):
    """The result reads as "here is your file" unless it says otherwise.

    `export_workbook` writes to the datalake host. Two agent runs took a path, a
    filename and a byte count, and told the owner the workbook was attached to a
    chat that had no attachment in it. The correction belongs in the result rather
    than only in the tool description: the description is read once, when the tool is
    chosen, and this is read at the moment the answer is written.
    """
    monkeypatch.setenv("DATALAKE_EXPORT_DIR", str(tmp_path / "out"))
    monkeypatch.setattr("homelab_datalake.mcp.deliverables.EXPORT_DIR", str(tmp_path / "out"))
    out = call(tools, "export_workbook", {
        "dataset": "camera_activity", "title": "Portal events",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
    })
    assert "error" not in out, out
    delivery = out["delivery"]
    assert "NOT attached" in delivery
    assert "create_excel" in delivery
    assert out["path"] in delivery
