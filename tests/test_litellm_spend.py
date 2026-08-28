"""LiteLLM extraction tests.

Rows are injected rather than read from a database, so these run anywhere. The
interesting assertions are about what does *not* come out.
"""

from datetime import UTC, date, datetime

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.litellm_spend import SELECT_COLUMNS, extract_day

SALT = "test-salt"
NO_GEO = GeoEnricher(None, None)


def row(**overrides):
    base = {
        "request_id": "req-1",
        "call_type": "acompletion",
        "api_key": "a" * 64,
        "spend": 0.0021,
        "total_tokens": 300,
        "prompt_tokens": 200,
        "completion_tokens": 100,
        "startTime": datetime(2026, 8, 27, 10, 0, 0),
        "endTime": datetime(2026, 8, 27, 10, 0, 2),
        "completionStartTime": datetime(2026, 8, 27, 10, 0, 1),
        "model": "openai/gpt-5.6-luna",
        "model_group": "chat",
        "custom_llm_provider": "openai",
        "cache_hit": "",
        "team_id": "team-a",
        "end_user": "someone@example.test",
        "status": "success",
        "request_duration_ms": 2000,
        "session_id": "sess-1",
        "mcp_namespaced_tool_name": None,
        "requester_ip_address": "203.0.113.7",
    }
    base.update(overrides)
    return base


def test_payload_columns_are_never_selected():
    """The guarantee, asserted against the query itself rather than its output.

    messages / response / proxy_server_request hold whole conversations. They are
    empty today only because payload logging happens to be off.
    """
    selected = " ".join(SELECT_COLUMNS).lower()
    for forbidden in ("messages", "response", "proxy_server_request"):
        assert forbidden not in selected


def test_caller_address_does_not_survive():
    address = "198.51.100.77"
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(requester_ip_address=address)])
    assert out.rows
    for r in out.rows:
        assert all(address not in str(v) for v in r.values())
    assert out.rows[0]["client_network"] == "198.51.100.0/24"


def test_api_key_is_hashed_even_though_it_may_already_be_one():
    """Nothing in the column says whether a value is a hash or a live credential --
    this deployment holds both shapes -- so it is treated as a credential."""
    literal = "litellm-key-abc123"
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(api_key=literal)])
    assert out.rows[0]["api_key_hash"] != literal
    assert len(out.rows[0]["api_key_hash"]) == 32
    assert all(literal not in str(v) for v in out.rows[0].values())


def test_end_user_is_hashed():
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(end_user="someone@example.test")])
    assert "example.test" not in str(out.rows[0])
    assert len(out.rows[0]["end_user"]) == 32


def test_naive_timestamps_are_treated_as_utc():
    """LiteLLM stores `timestamp without time zone`, already UTC. Leaving them naive
    would make the partition filter drift with the session timezone -- the bug that
    already cost a day once."""
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row()])
    assert out.rows[0]["started_at"].tzinfo == UTC
    assert out.rows[0]["started_at"] == datetime(2026, 8, 27, 10, 0, tzinfo=UTC)


def test_rows_outside_the_day_are_excluded():
    rows = [
        row(request_id="before", startTime=datetime(2026, 8, 26, 23, 59, 59)),
        row(request_id="inside", startTime=datetime(2026, 8, 27, 0, 0, 0)),
        row(request_id="after", startTime=datetime(2026, 8, 28, 0, 0, 0)),
    ]
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=rows)
    assert [r["request_id"] for r in out.rows] == ["inside"]


def test_totals_and_failures_are_counted():
    rows = [
        row(request_id="a", spend=0.10, status="success", model="m1"),
        row(request_id="b", spend=0.20, status="failure", model="m2"),
        row(request_id="c", spend=0.30, status="success", model="m1"),
    ]
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=rows)
    assert out.calls == 3
    assert round(out.total_spend, 6) == 0.60
    assert out.failures == 1
    assert out.models == {"m1", "m2"}


def test_missing_optional_fields_do_not_break_extraction():
    """Embedding and MCP calls leave several fields null; they are still real calls."""
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(
        completionStartTime=None, requester_ip_address=None, end_user=None,
        session_id=None, team_id=None, request_duration_ms=None, spend=0.0,
    )])
    r = out.rows[0]
    assert r["first_token_at"] is None and r["client_hash"] is None
    assert r["end_user"] is None and r["spend_usd"] == 0.0
    assert r["client_trust"] == "unknown"


def test_extraction_is_idempotent():
    rows = [row(request_id=f"r{i}") for i in range(10)]
    a = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=rows)
    b = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=rows)
    assert a.rows == b.rows
