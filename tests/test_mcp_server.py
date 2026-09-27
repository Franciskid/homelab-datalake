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
from homelab_datalake.mcp.semantic import DATASETS
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


def _size(payload) -> int:
    return len(json.dumps(payload, separators=(",", ":")))


# --------------------------------------------------------------------------------
# the tool surface itself
# --------------------------------------------------------------------------------


def test_every_tool_is_registered(tools):
    names = {t.name for t in asyncio.run(tools.list_tools())}
    assert names == {
        "list_datasets", "describe_dataset", "describe_schema", "query_dataset",
        "export_workbook",
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


def test_query_dataset_enum_matches_the_registry(tools):
    """A dataset added to DATASETS must reach the tool with no second edit.

    This is the regression the whole workstream exists for: `media_playback` and
    eight others were fully described by list_datasets/describe_dataset and had no
    way to be queried, because nobody had hand-written them a tool. The enum being
    generated from the registry, rather than typed out, is what makes that
    impossible to repeat.
    """
    tool = next(t for t in asyncio.run(tools.list_tools()) if t.name == "query_dataset")
    enum = set(tool.input_schema["properties"]["dataset"]["enum"])
    assert enum == set(DATASETS)


def test_query_dataset_description_keeps_the_old_tools_caveats(tools):
    """Retiring the four per-dataset tools must not lose what their descriptions said.

    The caveats live on the `dataset` parameter's own schema description -- the
    per-dataset catalogue -- not on the tool's top-level description.
    """
    tool = next(t for t in asyncio.run(tools.list_tools()) if t.name == "query_dataset")
    catalogue = tool.input_schema["properties"]["dataset"]["description"]
    assert "state_changed=true" in catalogue
    assert "no footage" in catalogue
    assert "never sum counts across days" in catalogue


def test_export_workbook_description_points_at_query_dataset(tools):
    tool = next(t for t in asyncio.run(tools.list_tools()) if t.name == "export_workbook")
    assert "query_dataset" in tool.description


# --------------------------------------------------------------------------------
# refusing an argument no tool declared
# --------------------------------------------------------------------------------
#
# The regression these exist for: an agent called `media_library` with an extra
# `dataset` argument for eight runs straight, and FastMCP's default -- an unknown
# key is dropped, not refused -- meant the call quietly ran against the wrong
# dataset instead of failing with a message that would have said why. These go
# through `call_tool`, the same path a real client uses, not the bare Python
# function -- the fix lives in the schema validation, not in the tool body.


def test_an_unknown_argument_is_refused_not_dropped(tools):
    with pytest.raises(Exception) as raised:
        asyncio.run(tools.call_tool("describe_schema", {"dataset": "media_playback"}))
    message = str(raised.value)
    assert "dataset" in message
    assert "table" in message  # the one real parameter describe_schema has


def test_an_unknown_argument_names_the_valid_ones(tools):
    with pytest.raises(Exception) as raised:
        asyncio.run(tools.call_tool("query_dataset", {
            "dataset": "llm_spend", "start": "2026-08-01", "end": "2026-08-20",
            "not_a_real_argument": "oops",
        }))
    message = str(raised.value)
    assert "not_a_real_argument" in message
    assert "start" in message and "dataset" in message  # the valid names, listed


def test_a_real_argument_still_works_once_hardened(tools):
    """The fix must not catch anything real on its way past."""
    payload = call(tools, "query_dataset", {
        "dataset": "llm_spend", "start": "2026-08-01", "end": "2026-08-20",
        "grain": "total", "dimensions": ["provider"], "measures": ["calls"],
    })
    assert "error" not in payload, payload
    assert payload["row_count"] == 3


# --------------------------------------------------------------------------------
# list_datasets
# --------------------------------------------------------------------------------


def test_listing_datasets_reports_what_each_one_covers(tools):
    payload = call(tools, "list_datasets")
    covered = {d["name"]: d["covers"] for d in payload["datasets"]}
    assert covered["llm_spend"] == {"from": "2026-08-01", "to": "2026-08-20"}
    assert "total" in payload["grains"]
    assert {d["name"] for d in payload["datasets"]} == set(DATASETS)
    kinds = {d["name"]: d["kind"] for d in payload["datasets"]}
    assert kinds["media_library"] == "snapshot"
    assert kinds["llm_spend"] == "event"


def test_list_datasets_stays_inside_its_budget(tools):
    payload = call(tools, "list_datasets")
    assert _size(payload) < server_module.LIST_DATASETS_BUDGET, _size(payload)


# --------------------------------------------------------------------------------
# describe_dataset: compact (default), full, and names=[...]
# --------------------------------------------------------------------------------


def test_describe_dataset_compact_is_the_default(tools):
    payload = call(tools, "describe_dataset", {"dataset": "camera_activity"})
    assert payload["name"] == "camera_activity"
    assert payload["view"] == "datasets.camera_activity"
    assert payload["table"] == "marts.fct_camera_event"
    # Compact entries are plain strings, not {"name": ..., "sql": ...} objects --
    # the sql key dimensions and fields used to carry is gone, because the view
    # named above makes it unnecessary.
    assert all(isinstance(line, str) for line in payload["dimensions"])
    assert all(isinstance(line, str) for line in payload["fields"])
    assert any(line.startswith("lifecycle") for line in payload["dimensions"])
    # Dimensions are not repeated under fields in compact mode.
    assert not any(line.startswith("lifecycle") for line in payload["fields"])


def test_describe_dataset_compact_stays_inside_its_budget(tools):
    for name in DATASETS:
        payload = call(tools, "describe_dataset", {"dataset": name})
        size = _size(payload)
        assert size < server_module.DESCRIBE_DATASET_BUDGET, (name, size)


def test_describe_dataset_full_shows_the_sql_behind_a_renamed_dimension(tools):
    """The regression this whole layer exists for.

    `person` on media_playback is `user_name` on the mart. Production shipped this
    dataset showing only `person`, an agent wrote `SELECT person FROM
    marts.fct_jellyfin_playback`, and got a Binder Error twice.
    """
    payload = call(tools, "describe_dataset", {"dataset": "media_playback", "detail": "full"})
    dims = {d["name"]: d for d in payload["dimensions"]}
    assert dims["person"]["sql"] == "user_name"
    assert dims["series"]["sql"] == "series_name"
    assert dims["media_type"]["sql"] == "item_type"
    assert "sql" not in dims["title"]


def test_describe_dataset_full_always_shows_a_measures_sql(tools):
    payload = call(tools, "describe_dataset", {"dataset": "media_playback", "detail": "full"})
    measures = {m["name"]: m for m in payload["measures"]}
    assert measures["watch_hours"]["sql"] == (
        "round((sum(play_duration_seconds) / 3600.0)::numeric, 2)"
    )
    assert all("sql" in m for m in payload["measures"])


def test_describe_dataset_names_returns_full_detail_for_just_those_names(tools):
    payload = call(tools, "describe_dataset", {
        "dataset": "media_playback", "names": ["person", "watch_hours"],
    })
    assert [d["name"] for d in payload["dimensions"]] == ["person"]
    assert payload["dimensions"][0]["sql"] == "user_name"
    assert [m["name"] for m in payload["measures"]] == ["watch_hours"]
    # "person" is a dimension, and every dimension is also usable as a field at
    # grain='event' -- so it is named here too, with the same sql.
    assert [f["name"] for f in payload["fields"]] == ["person"]
    assert payload["fields"][0]["sql"] == "user_name"
    assert "not_found" not in payload


def test_describe_dataset_names_reports_a_name_that_matched_nothing(tools):
    payload = call(tools, "describe_dataset", {
        "dataset": "media_playback", "names": ["not_a_real_name"],
    })
    assert "not_a_real_name" in payload["not_found"]


def test_a_high_cardinality_dimension_compact_carries_no_enum(tools):
    """A column with dozens of values must not fill compact output with a list."""
    payload = call(tools, "describe_dataset", {"dataset": "media_library"})
    # 30 production years in the fixture, above COMPACT_MAX_VALUES.
    year_line = next(line for line in payload["dimensions"] if line.startswith("production_year"))
    assert "=" not in year_line
    # A small one is still shown in full.
    resolution_line = next(
        line for line in payload["dimensions"] if line.startswith("resolution_class")
    )
    assert "4K" in resolution_line and "1080p" in resolution_line


def test_a_high_cardinality_dimension_full_is_summarised_not_dumped(tools):
    payload = call(tools, "describe_dataset", {"dataset": "media_library", "detail": "full"})
    values = {d["name"]: d["values"] for d in payload["dimensions"]}
    assert isinstance(values["production_year"], str)
    assert "many distinct values" in values["production_year"]
    assert set(values["resolution_class"]) == {"4K", "1080p", "720p"}


def test_an_unknown_dataset_names_the_real_ones(tools):
    payload = call(tools, "describe_dataset", {"dataset": "spending"})
    assert "llm_spend" in payload["error"]


# --------------------------------------------------------------------------------
# describe_schema
# --------------------------------------------------------------------------------


def test_describe_schema_lists_datasets_first_and_the_marts(tools):
    out = call(tools, "describe_schema")
    assert any(line.startswith("datasets.llm_spend") for line in out["datasets"])
    assert isinstance(out["tables"], list) and out["tables"]
    assert "datasets.*" in out["preferred"] or "datasets" in out["preferred"]


def test_describe_schema_stays_inside_its_budget(tools):
    out = call(tools, "describe_schema")
    assert _size(out) < server_module.DESCRIBE_SCHEMA_BUDGET, _size(out)


def test_describe_schema_of_a_dataset_view_points_at_describe_dataset(tools):
    out = call(tools, "describe_schema", {"table": "llm_spend"})
    assert out["query_as"] == "datasets.llm_spend"
    assert any(c["name"] == "model" for c in out["columns"])
    assert "describe_dataset" in out["note"]


def test_describe_schema_of_a_mart_table_carries_its_columns(tools):
    out = call(tools, "describe_schema")
    one = out["tables"][0].split(" -- ")[0]
    detail = call(tools, "describe_schema", {"table": one})
    assert detail["query_as"] == one


def test_describe_schema_unknown_table_names_close_matches(tools):
    out = call(tools, "describe_schema", {"table": "llm_spned"})
    assert "llm_spend" in out["error"]


# --------------------------------------------------------------------------------
# query_dataset
# --------------------------------------------------------------------------------


def test_a_question_returns_rows_and_the_sql_that_produced_them(tools):
    payload = call(tools, "query_dataset", {
        "dataset": "llm_spend", "start": "2026-08-01", "end": "2026-08-20", "grain": "total",
        "dimensions": ["provider"], "measures": ["calls", "spend_usd"],
    })
    assert payload["row_count"] == 3
    assert "SELECT" in payload["sql"]


def test_a_bad_call_returns_the_reason_rather_than_raising(tools):
    """The model fixes its own call from this message; a stack trace teaches it nothing."""
    payload = call(tools, "query_dataset", {
        "dataset": "llm_spend", "start": "2026-08-01", "end": "2026-08-20", "measures": ["revenue"],
    })
    assert "revenue" in payload["error"]
    assert "Available" in payload["error"]
    assert payload["row_count"] == 0


def test_an_unavailable_field_routes_to_the_dataset_that_has_it(tools):
    """The fix for the failure this whole change started from: an agent called
    `media_library` with `media_playback`'s field names, eight times, with no hint
    about which dataset it should have called instead."""
    payload = call(tools, "query_dataset", {
        "dataset": "media_library", "start": "2026-08-25", "end": "2026-08-27",
        "grain": "event", "fields": ["started_at", "title"],
    })
    assert "media_playback" in payload["error"]
    assert "It exists on" in payload["error"]


def test_an_unknown_dataset_is_refused_by_the_schema(tools):
    """The `dataset` parameter is a JSON-schema enum, so this fails before any
    query is built -- and the enum's own error already names the real ones."""
    with pytest.raises(Exception) as raised:
        asyncio.run(tools.call_tool("query_dataset", {
            "dataset": "spending", "start": "2026-08-01", "end": "2026-08-20",
        }))
    assert "llm_spend" in str(raised.value)


def test_a_boolean_dimension_can_be_filtered_with_a_boolean(tools):
    """The schema must accept the values describe_dataset advertises.

    `state_changed` is reported as `[false, true]`. Declaring filters as
    `dict[str, list[str]]` meant filtering on `true` was refused before it reached
    the query layer -- "Input validation error: True is not of type 'string'". An
    agent that hit that could not tell what was wrong, concluded its range was too
    wide, and split one correct query into thirty until it ran out of steps.
    """
    booleans = call(tools, "query_dataset", {
        "dataset": "camera_activity",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
        "fields": ["happened_at", "zone_state"],
        "filters": {"state_changed": [True]},
    })
    assert "error" not in booleans, booleans
    assert booleans["row_count"] > 0

    # The string form keeps working, because DuckDB coerces it and because a caller
    # that learned the old shape should not break.
    strings = call(tools, "query_dataset", {
        "dataset": "camera_activity",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "event",
        "fields": ["happened_at", "zone_state"],
        "filters": {"state_changed": ["true"]},
    })
    assert strings["row_count"] == booleans["row_count"]


def test_a_numeric_dimension_can_be_filtered_with_a_number(tools):
    payload = call(tools, "query_dataset", {
        "dataset": "camera_activity",
        "start": "2026-08-01", "end": "2026-08-20", "grain": "total",
        "dimensions": ["started_hour_of_day"], "measures": ["events"],
        "filters": {"started_hour_of_day": [7, 8]},
    })
    assert "error" not in payload, payload
    assert {row["started_hour_of_day"] for row in payload["rows"]} <= {7, 8}


def test_query_dataset_reaches_a_dataset_the_old_tools_never_had(tools):
    """media_playback had no tool at all before this change -- list_datasets and
    describe_dataset described it, and nothing could query it."""
    payload = call(tools, "query_dataset", {
        "dataset": "media_playback", "start": "2026-08-01", "end": "2026-08-08",
        "grain": "event", "fields": ["started_at", "person", "title", "series", "media_type"],
    })
    assert "error" not in payload, payload
    assert payload["row_count"] > 0


# --------------------------------------------------------------------------------
# export_workbook
# --------------------------------------------------------------------------------


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
