"""Extraction tests for the ai_platform source.

Same contract as litellm_spend's tests: rows are injected, so nothing here needs a
database, and psycopg is only ever reached from `extract_table_day` when `rows` is
left as `None` -- which no test here does.
"""

import base64
import uuid
from datetime import UTC, date, datetime

import pytest

from homelab_datalake.parsing import ai_platform as ap
from homelab_datalake.parsing.ai_platform_registry import spec

_TINY_JPEG_B64 = (
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0a"
    "HBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIy"
    "MjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy/8AAEQgAAQABAwEiAAIR"
    "AQMRAf/EABQAAQAAAAAAAAAAAAAAAAAAAAj/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAA"
    "AAAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCdABmX/9k="
)
_TINY_JPEG_BYTES = base64.b64decode(_TINY_JPEG_B64)


# --------------------------------------------------------------------------------
# build_query
# --------------------------------------------------------------------------------


def test_window_mode_query_filters_on_the_change_column():
    sql = ap.build_query(spec("runs"))
    assert 'FROM "runs"' in sql
    assert 'WHERE "started_at" >= %(start)s AND "started_at" < %(end)s' in sql
    assert "ORDER BY" in sql


def test_snapshot_mode_query_has_no_where_clause():
    sql = ap.build_query(spec("agents"))
    assert 'FROM "agents"' in sql
    assert "WHERE" not in sql


def test_query_selects_named_columns_never_star():
    sql = ap.build_query(spec("applications"))
    assert "select *" not in sql.lower()
    for column in spec("applications").columns:
        assert f'"{column}"' in sql


# --------------------------------------------------------------------------------
# window mode: the day filter, and idempotency
# --------------------------------------------------------------------------------


def _runs_row(**overrides) -> dict:
    base = {
        "id": uuid.UUID(int=1),
        "conversation_id": uuid.UUID(int=2),
        "initiated_by_user_id": uuid.UUID(int=3),
        "status": "completed",
        "requested_model": "claude",
        "actual_route": "claude-opus",
        "input_tokens": 10,
        "output_tokens": 20,
        "cost_eur": 0.5,
        "started_at": datetime(2026, 9, 14, 10, 0, tzinfo=UTC),
        "completed_at": datetime(2026, 9, 14, 10, 0, 5, tzinfo=UTC),
        "failure_code": None,
        "metadata": {"a": 1},
        "awaiting_input_deadline": None,
        "report_definition_id": None,
        "reasoning_tokens": None,
    }
    base.update(overrides)
    return base


def test_extraction_lands_the_partition_day():
    out = ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=[_runs_row()])
    assert out.rows[0]["dt"] == date(2026, 9, 14)
    assert out.count == 1


def test_extraction_is_idempotent():
    rows = [_runs_row(id=uuid.UUID(int=i)) for i in range(10)]
    a = ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=rows)
    b = ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=rows)
    assert a.rows == b.rows
    assert a.count == b.count == 10


def test_extraction_does_not_mutate_the_injected_rows():
    """rows is a fixture in tests and the caller's own data in real use; either way
    extract_table_day must not edit it in place."""
    row = _runs_row()
    original = dict(row)
    ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=[row])
    assert row == original


# --------------------------------------------------------------------------------
# value coercion: jsonb -> JSON text, uuid -> string, tz-naive -> tz-aware
# --------------------------------------------------------------------------------


def test_jsonb_dict_values_are_dumped_to_json_text():
    out = ap.extract_table_day(
        spec("runs"), date(2026, 9, 14), rows=[_runs_row(metadata={"reasoning": "why", "n": 3})]
    )
    assert out.rows[0]["metadata"] == '{"n": 3, "reasoning": "why"}'


def test_a_jsonb_list_is_also_dumped_to_json_text():
    out = ap.extract_table_day(
        spec("mcp_servers"), date(2026, 9, 14), rows=[{
            "id": uuid.UUID(int=1), "created_by_user_id": uuid.UUID(int=2), "name": "x",
            "transport": "http", "url": "https://example.test/mcp", "status": "active",
            "tool_count": 2, "allowed_tools": ["a", "b"], "auth_fingerprint": "f",
            "auth_updated_at": None,
            "created_at": datetime(2026, 9, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 9, 2, tzinfo=UTC),
        }],
    )
    assert out.rows[0]["allowed_tools"] == '["a", "b"]'


def test_uuid_values_become_plain_strings():
    out = ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=[_runs_row()])
    assert out.rows[0]["id"] == "00000000-0000-0000-0000-000000000001"
    assert isinstance(out.rows[0]["id"], str)


def test_a_naive_timestamp_is_treated_as_utc():
    """ai_platform's own timestamptz columns should arrive tz-aware already (the
    connection pins UTC), but a naive value must still land safely rather than
    crash the run if one ever slips through."""
    out = ap.extract_table_day(
        spec("runs"), date(2026, 9, 14),
        rows=[_runs_row(started_at=datetime(2026, 9, 14, 10, 0))],
    )
    assert out.rows[0]["started_at"].tzinfo == UTC


def test_null_values_stay_null():
    out = ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=[_runs_row(metadata=None)])
    assert out.rows[0]["metadata"] is None
    assert out.rows[0]["failure_code"] is None


# --------------------------------------------------------------------------------
# redaction: mcp_servers.url loses its query string, nothing else does
# --------------------------------------------------------------------------------


def _mcp_row(**overrides) -> dict:
    base = {
        "id": uuid.UUID(int=1), "created_by_user_id": uuid.UUID(int=2), "name": "docs",
        "transport": "http", "url": "https://mcp.example.test/sse?token=SECRET123&x=1",
        "status": "active", "tool_count": 3, "allowed_tools": ["a"],
        "auth_fingerprint": "fp", "auth_updated_at": None,
        "created_at": datetime(2026, 9, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 9, 2, tzinfo=UTC),
    }
    base.update(overrides)
    return base


def test_mcp_servers_url_loses_its_query_string():
    out = ap.extract_table_day(spec("mcp_servers"), date(2026, 9, 14), rows=[_mcp_row()])
    assert out.rows[0]["url"] == "https://mcp.example.test/sse"
    assert "SECRET123" not in str(out.rows[0])
    assert "token" not in out.rows[0]["url"]


def test_mcp_servers_url_with_no_query_string_is_unchanged():
    out = ap.extract_table_day(
        spec("mcp_servers"), date(2026, 9, 14),
        rows=[_mcp_row(url="https://mcp.example.test/sse")],
    )
    assert out.rows[0]["url"] == "https://mcp.example.test/sse"


def test_mcp_command_and_metadata_are_never_in_the_output_row():
    """Excluded columns are never selected, so even if the injected fixture row
    carried them (as a real Postgres row would), they cannot appear on the output --
    extract_table_day only ever reads TableSpec.columns."""
    row = _mcp_row()
    row["command"] = "node server.js --api-key sk-should-never-appear"
    row["metadata"] = {"apiKey": "sk-should-never-appear-either"}
    out = ap.extract_table_day(spec("mcp_servers"), date(2026, 9, 14), rows=[row])
    landed = out.rows[0]
    assert "command" not in landed
    assert "sk-should-never-appear" not in str(landed)
    assert "sk-should-never-appear-either" not in str(landed)


# --------------------------------------------------------------------------------
# binary columns: message_attachments / generated_files
# --------------------------------------------------------------------------------


def test_binary_column_is_replaced_by_a_deterministic_object_key():
    out = ap.extract_table_day(
        spec("generated_files"), date(2026, 9, 14), rows=[{
            "id": uuid.UUID(int=7), "conversation_id": None, "run_id": None,
            "filename": "report.xlsx", "mime_type": "application/xlsx", "byte_size": 4,
            "data": b"\x01\x02\x03\x04", "created_at": datetime(2026, 9, 14, tzinfo=UTC),
            "tool_name": None, "server_alias": None,
        }],
    )
    row = out.rows[0]
    assert "data" not in row
    assert row["object_key"] == (
        "files/ai_platform/generated_files/dt=2026-09-14/"
        "00000000-0000-0000-0000-000000000007"
    )
    assert row["_binary_data"] == b"\x01\x02\x03\x04"


def test_binary_column_null_data_yields_no_object_key():
    out = ap.extract_table_day(
        spec("message_attachments"), date(2026, 9, 14), rows=[{
            "id": uuid.UUID(int=1), "conversation_id": None, "message_id": None,
            "uploaded_by_user_id": None, "mime_type": "image/png", "byte_size": None,
            "data": None, "created_at": datetime(2026, 9, 14, tzinfo=UTC),
        }],
    )
    row = out.rows[0]
    assert row["object_key"] is None
    assert "_binary_data" not in row


def test_re_running_the_same_day_produces_the_same_object_key():
    """Idempotency for a binary table: the object key must not depend on when the
    extraction ran, only on the day and the row's own id."""
    row = {
        "id": uuid.UUID(int=3), "conversation_id": None, "run_id": None,
        "filename": "x.csv", "mime_type": "text/csv", "byte_size": 2,
        "data": b"ok", "created_at": datetime(2026, 9, 14, tzinfo=UTC),
        "tool_name": None, "server_alias": None,
    }
    a = ap.extract_table_day(spec("generated_files"), date(2026, 9, 14), rows=[row])
    b = ap.extract_table_day(spec("generated_files"), date(2026, 9, 14), rows=[dict(row)])
    assert a.rows[0]["object_key"] == b.rows[0]["object_key"]


# --------------------------------------------------------------------------------
# usage_events: frames end to end through extract_table_day
# --------------------------------------------------------------------------------


def _usage_event_row(**overrides) -> dict:
    base = {
        "id": uuid.UUID(int=42), "application_id": None, "api_key_id": None,
        "source": "gateway", "client_name": None, "client_version": None,
        "actor_label": None, "actor_external_id": None, "requested_model": "m",
        "actual_model": "m2", "prompt": "hi", "response": "hello", "results": None,
        "input_tokens": 1, "output_tokens": 2, "cost_eur": 0.01, "latency_ms": 500,
        "status": "success", "failure_code": None,
        "metadata": {"images": [], "clipUrl": "https://example.test/clip.mp4"},
        "created_at": datetime(2026, 9, 14, 12, 0, tzinfo=UTC), "conversation_id": "conv-1",
    }
    base.update(overrides)
    return base


def test_usage_events_row_with_no_frames_reports_zero():
    out = ap.extract_table_day(spec("usage_events"), date(2026, 9, 14), rows=[_usage_event_row()])
    row = out.rows[0]
    assert row["frame_count"] == 0
    assert row["frame_bytes"] == 0
    assert row["frame_keys"] == []
    assert row["metadata"] == '{"clipUrl": "https://example.test/clip.mp4"}'


def test_usage_events_decodes_frames_and_counts_malformed_ones():
    row = _usage_event_row(metadata={
        "images": [
            f"data:image/jpeg;base64,{_TINY_JPEG_B64}",
            "not-a-real-frame",
            f"data:image/jpeg;base64,{_TINY_JPEG_B64}",
        ],
        "clipUrl": "https://example.test/clip.mp4",
    })
    out = ap.extract_table_day(spec("usage_events"), date(2026, 9, 14), rows=[row])
    landed = out.rows[0]
    assert landed["frame_count"] == 2
    assert landed["frame_bytes"] == 2 * len(_TINY_JPEG_BYTES)
    assert landed["frame_keys"] == [
        "files/ai_platform/gateway_frames/dt=2026-09-14/00000000-0000-0000-0000-00000000002a/0.jpg",
        "files/ai_platform/gateway_frames/dt=2026-09-14/00000000-0000-0000-0000-00000000002a/1.jpg",
    ]
    assert out.frame_decode_errors == 1
    # never in the row itself -- popped by the caller before the Arrow table is built
    assert landed["_frames"] and len(landed["_frames"]) == 2


def test_a_day_with_every_frame_malformed_still_lands_the_row():
    """The whole point: one bad frame -- or every frame -- must not fail the day."""
    row = _usage_event_row(metadata={"images": ["nope", None, 123, "data:image/jpeg,x"]})
    out = ap.extract_table_day(spec("usage_events"), date(2026, 9, 14), rows=[row])
    assert out.count == 1
    assert out.rows[0]["frame_count"] == 0
    assert out.frame_decode_errors == 4


# --------------------------------------------------------------------------------
# the secret scan, exercised against extraction output
# --------------------------------------------------------------------------------


def test_ordinary_extracted_rows_carry_nothing_that_looks_like_a_secret():
    rows = [
        ap.extract_table_day(spec("runs"), date(2026, 9, 14), rows=[_runs_row()]).rows[0],
        ap.extract_table_day(spec("mcp_servers"), date(2026, 9, 14), rows=[_mcp_row()]).rows[0],
    ]
    findings = ap.scan_for_secrets(rows, skip_columns=frozenset({"metadata"}))
    assert findings == []


def test_the_scanner_actually_catches_a_planted_secret():
    """A test that never fails is not a test. This proves the scanner finds the
    kind of value it exists to find, in a column that is not excluded from
    extraction -- unlike the real secret columns, which extract_table_day never
    reads at all (see test_mcp_command_and_metadata_are_never_in_the_output_row)."""
    rows = [{"label": "sk-" + "a" * 32, "other": "fine"}]
    findings = ap.scan_for_secrets(rows)
    assert findings == [(0, "label")]


def test_full_prompt_and_response_text_does_not_false_positive():
    """prompt/response can be arbitrarily long free text; it must not be mistaken
    for a secret just because it is long."""
    long_prose = ("The quick brown fox jumps over the lazy dog. " * 20)
    out = ap.extract_table_day(
        spec("usage_events"), date(2026, 9, 14),
        rows=[_usage_event_row(prompt=long_prose, response=long_prose)],
    )
    findings = ap.scan_for_secrets(out.rows, skip_columns=frozenset({"metadata"}))
    assert findings == []


@pytest.mark.parametrize("table_name", ["api_keys", "providers", "push_subscriptions",
                                         "platform_settings", "user_tool_configs"])
def test_tables_with_a_hard_excluded_secret_never_select_it(table_name):
    """Belt and braces: for every table carrying a required secret exclusion,
    confirm the selected columns really do not include it -- exercised through the
    registry rather than assumed."""
    from tests.test_ai_platform_registry import REQUIRED_SECRET_EXCLUSIONS

    table = spec(table_name)
    excluded_here = {col for (t, col) in REQUIRED_SECRET_EXCLUSIONS if t == table_name}
    assert excluded_here, f"no required exclusion listed for {table_name} in the test itself"
    assert excluded_here.isdisjoint(table.columns)
