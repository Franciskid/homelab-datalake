"""LiteLLM extraction tests.

Rows are injected rather than read from a database, so these run anywhere. The
interesting assertions are about what does *not* come out.
"""

import json
from datetime import UTC, date, datetime

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.litellm_spend import (
    METADATA_NOT_STORED,
    SELECT_COLUMNS,
    extract_day,
)

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


def test_payload_columns_are_selected_and_landed():
    """The whole ledger, by decision of the lake's owner.

    These hold whole conversations, and they are empty today only because payload
    logging happens to be off -- so selecting them is a decision about the future
    rather than about the current contents. Asserted against the query itself, because
    the output cannot tell "not selected" from "selected and empty".
    """
    selected = " ".join(SELECT_COLUMNS).lower()
    for column in ("messages", "response", "proxy_server_request"):
        assert column in selected

    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(
        messages=[{"role": "user", "content": "what is the gate doing"}],
        response={"choices": [{"message": {"content": "It is closed."}}]},
    )])
    assert "what is the gate doing" in out.rows[0]["messages_json"]
    assert "It is closed." in out.rows[0]["response_json"]


def test_an_empty_payload_lands_as_null_rather_than_an_empty_object():
    """Which is the state of every row today, and should read as "nothing" not "{}"."""
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(messages={}, response=None)])
    assert out.rows[0]["messages_json"] is None
    assert out.rows[0]["response_json"] is None


def test_the_caller_address_is_landed_raw_and_hashed():
    """Both, because they answer different questions.

    On this deployment the address is the internal container that called the gateway
    -- a fact about one's own infrastructure. The proxy logs next door are the
    opposite case, and keep their hashing boundary.
    """
    address = "198.51.100.77"
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(requester_ip_address=address)])
    assert out.rows[0]["requester_ip"] == address
    assert out.rows[0]["client_hash"] and out.rows[0]["client_hash"] != address
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


# --------------------------------------------------------------------------------
# the metadata blob
# --------------------------------------------------------------------------------
#
# `metadata` is populated on every row in this ledger and carries figures nothing
# else does. It also carries the requester's address in the clear and the name of the
# key that made the call, so it is the one place where "keep everything" and the
# privacy boundary actually collide.


def _metadata(**overrides):
    base = {
        "usage_object": {
            "total_tokens": 753,
            "prompt_tokens": 682,
            "completion_tokens": 71,
            "prompt_tokens_details": {"audio_tokens": 0, "cached_tokens": 512},
            "completion_tokens_details": {"reasoning_tokens": 25, "audio_tokens": 0},
        },
        "cost_breakdown": {"input_cost": 0.0001364, "output_cost": 0.0000852,
                           "total_cost": 0.0002216},
        "attempted_retries": 1,
        "max_retries": 3,
        "applied_guardrails": ["pii-filter"],
        "user_api_key_alias": "workbench",
        "model_map_information": {"model_map_key": "demo-model"},
        # The three that must not survive.
        "requester_ip_address": "203.0.113.44",
        "user_api_key": "litellm_proxy_master_key",
        "proxy_server_request": {"body": {"messages": [{"role": "user", "content": "secret"}]}},
    }
    base.update(overrides)
    return base


def test_the_token_detail_the_lake_had_no_column_for_is_extracted():
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(metadata=_metadata())])
    call = out.rows[0]
    assert call["cached_tokens"] == 512
    assert call["reasoning_tokens"] == 25
    assert call["input_cost_usd"] == 0.0001364
    assert call["output_cost_usd"] == 0.0000852
    assert call["attempted_retries"] == 1
    assert call["applied_guardrails"] == ["pii-filter"]
    assert call["api_key_alias"] == "workbench"
    assert call["model_map_key"] == "demo-model"


def test_zero_and_not_reported_stay_different_facts():
    """A provider that does not report reasoning tokens is not a model that used none."""
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[
        row(metadata={"usage_object": {"completion_tokens_details": {"reasoning_tokens": 0}}}),
        row(request_id="r2", metadata={"usage_object": {}}),
    ])
    assert out.rows[0]["reasoning_tokens"] == 0
    assert out.rows[1]["reasoning_tokens"] is None


def test_the_metadata_blob_is_landed_whole():
    """Nothing is filtered out of it -- that is the current decision.

    `METADATA_NOT_STORED` is empty rather than gone, so re-introducing a filter is one
    name in a set; this asserts the mechanism still works, and that nothing is being
    dropped today.
    """
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(metadata=_metadata())])
    kept = json.loads(out.rows[0]["metadata_json"])

    assert set(kept) == set(_metadata()), "the blob lost a key on the way through"
    assert set(kept).isdisjoint(METADATA_NOT_STORED)
    assert kept["requester_ip_address"] == "203.0.113.44"
    assert "usage_object" in kept
    # The pseudonymised form is still there beside it, so nothing downstream that was
    # written against the hash has to change.
    assert out.rows[0]["client_hash"]


def test_the_api_key_is_still_the_one_thing_held_back():
    """A working credential in a readable table is not the same as data being available.

    The alias travels in the clear, which is what a person recognises; the key itself
    is hashed, because nothing in the column says whether a given value is a hash
    already or a live key.
    """
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(
        api_key="litellm-key-abc123", metadata=_metadata(),
    )])
    assert out.rows[0]["api_key_hash"] != "litellm-key-abc123"
    assert out.rows[0]["api_key_alias"] == "workbench"


def test_a_row_with_no_metadata_is_not_an_error():
    out = extract_day(date(2026, 8, 27), SALT, NO_GEO, rows=[row(metadata=None)])
    call = out.rows[0]
    assert call["reasoning_tokens"] is None
    assert call["metadata_json"] is None
    assert call["applied_guardrails"] == []
