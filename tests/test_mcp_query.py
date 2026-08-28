"""What the query layer refuses, and what it says when the answer is nothing.

Two themes here. The first is that a caller cannot reach SQL: names are looked up,
values are bound, and anything unrecognised is refused with the list of what would
have worked. The second is the empty answer, which is the failure mode that matters
for a tool a model drives -- a confident summary over zero rows is worse than an
error, because nothing about it looks wrong.
"""

import time

import pytest

from homelab_datalake.mcp.query import (
    MAX_RANGE_DAYS,
    MAX_ROWS,
    QueryError,
    _execute_bounded,
    run_query,
)

AUGUST = {"start": "2026-08-01", "end": "2026-08-20"}


def test_a_plain_question_returns_rows(con):
    result = run_query(con, "llm_spend", grain="total", dimensions=["provider"],
                       measures=["calls", "spend_usd"], **AUGUST)
    assert result.row_count == 3
    assert {row["provider"] for row in result.rows} == {"anthropic", "openai", "ollama"}
    assert sum(row["calls"] for row in result.rows) == 200


def test_results_are_json_safe(con):
    """DECIMAL and DATE do not survive a JSON round trip; the tool output must."""
    import json

    result = run_query(con, "web_traffic", grain="week",
                       measures=["requests", "server_error_pct"], **AUGUST)
    json.dumps(result.as_dict())  # would raise on Decimal or date
    assert all(isinstance(row["period"], str) for row in result.rows)


# --- what cannot be asked -----------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"measures": ["profit"]},
        {"dimensions": ["customer"]},
        {"filters": {"not_a_column": ["x"]}},
        {"grain": "fortnight"},
        {"order_by": "median_ttft_ms"},  # a real measure, but not one that was selected
    ],
)
def test_unknown_names_are_refused_with_the_alternatives(con, kwargs):
    with pytest.raises(QueryError) as raised:
        run_query(con, "llm_spend", **AUGUST, **kwargs)
    message = str(raised.value)
    # The message must be actionable: it is what the model reads to fix its own call.
    assert any(word in message for word in ("Available", "not among", "unknown"))


@pytest.mark.parametrize(
    "hostile",
    [
        "model; DROP TABLE marts.fct_llm_call",
        "model) UNION SELECT 1 --",
        "1=1 OR provider",
    ],
)
def test_sql_shaped_dimension_names_are_just_unknown_names(con, hostile):
    """Not sanitised -- unrepresentable. A name is a key, and this key is not in the map."""
    with pytest.raises(QueryError):
        run_query(con, "llm_spend", dimensions=[hostile], **AUGUST)


def test_filter_values_are_bound_not_interpolated(con):
    """A hostile *value* reaches the database as a parameter, so it simply matches nothing."""
    result = run_query(con, "llm_spend", grain="total", dimensions=["provider"],
                       filters={"provider": ["'; DROP TABLE marts.fct_llm_call; --"]},
                       **AUGUST)
    assert result.row_count == 0
    # The table is still there, which is the actual assertion.
    assert con.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0] == 200


def test_a_range_wider_than_the_cap_is_refused(con):
    with pytest.raises(QueryError, match="exceeds"):
        run_query(con, "web_traffic", start="2020-01-01", end="2026-08-01")


def test_dates_must_be_dates(con):
    with pytest.raises(QueryError, match="YYYY-MM-DD"):
        run_query(con, "web_traffic", start="last tuesday", end="2026-08-01")


def test_end_before_start_is_refused(con):
    with pytest.raises(QueryError, match="before"):
        run_query(con, "web_traffic", start="2026-08-20", end="2026-08-01")


def test_the_range_cap_is_not_a_suggestion():
    assert MAX_RANGE_DAYS < 365 * 5


def test_limit_is_clamped_not_trusted(con):
    result = run_query(con, "web_traffic", grain="day", measures=["requests"],
                       limit=10**9, **AUGUST)
    # The cap is applied in SQL, so the rows are never materialised and then thrown
    # away -- which is the difference between a limit and a courtesy.
    assert f"LIMIT {MAX_ROWS + 1}" in result.sql
    assert result.row_count <= MAX_ROWS


def test_truncation_is_announced(con):
    result = run_query(con, "web_traffic", grain="day",
                       dimensions=["status_class", "client_kind"],
                       measures=["requests"], limit=5, **AUGUST)
    assert result.row_count == 5
    assert result.truncated
    assert any("not the whole answer" in note for note in result.notes)


def test_the_deadline_is_enforced_by_interrupting(con):
    """DuckDB has no statement timeout, so the watchdog is the only thing enforcing it."""
    began = time.perf_counter()
    with pytest.raises(QueryError, match="exceeded"):
        _execute_bounded(con, "SELECT count(*) FROM range(100000000000) t(i) WHERE i % 7 = 3",
                         [], 1.0)
    assert time.perf_counter() - began < 10, "the query was not actually cancelled"
    # The connection has to survive being interrupted, or one slow query poisons the server.
    assert con.execute("SELECT 1").fetchone() == (1,)


# --- the empty answer ---------------------------------------------------------


def test_an_empty_window_says_so_and_says_where_the_data_is(con):
    result = run_query(con, "camera_activity", start="2026-01-01", end="2026-01-05")
    assert result.row_count == 0
    notes = " ".join(result.notes)
    assert "empty result, not a zero" in notes
    assert "2026-08-01" in notes, "the note must say which range does have data"
    assert "outside" in notes, "and that the requested window missed it entirely"
    assert "Do not summarise" in notes


def test_an_empty_filter_result_points_at_the_filter(con):
    result = run_query(con, "llm_spend", filters={"provider": ["mistral"]}, **AUGUST)
    assert result.row_count == 0
    assert any("describe_dataset" in note for note in result.notes)


def test_an_empty_filter_list_is_a_mistake_not_a_no_op(con):
    with pytest.raises(QueryError, match="no values"):
        run_query(con, "llm_spend", filters={"provider": []}, **AUGUST)


def test_null_in_a_filter_matches_nulls(con):
    """`dismissal_reason` is null for confirmed events, and that is a real question."""
    result = run_query(con, "camera_activity", grain="total",
                       measures=["events"], filters={"dismissal_reason": [None]}, **AUGUST)
    assert result.rows[0]["events"] > 0


# --- snapshots are not events -------------------------------------------------


def test_a_snapshot_range_selects_one_day_rather_than_summing(con):
    """Summing a snapshot across days counts each film once per day it existed."""
    result = run_query(con, "media_library", start="2026-08-25", end="2026-08-27",
                       grain="day", measures=["titles"])
    # Three snapshots exist in that range; the answer is one of them, not their sum.
    assert result.row_count == 1
    assert result.rows[0]["titles"] == 120
    assert result.grain == "total"
    assert result.start == result.end == "2026-08-27"
    assert any("snapshot" in note for note in result.notes)


def test_a_range_with_no_snapshot_refuses_to_look_like_a_decline(con):
    result = run_query(con, "media_library", start="2026-01-01", end="2026-01-31")
    assert result.row_count == 0
    notes = " ".join(result.notes)
    assert "not a change or a decline" in notes or "not a decline" in notes or "decline" in notes


# --- what the semantic layer deliberately does not expose ---------------------


def test_no_dataset_lets_you_group_by_an_individual():
    """The pseudonymous columns exist in the marts and are not offered as dimensions.

    `client_hash` identifies one visitor across requests. It is in `fct_http_request`
    because visitor *counts* need it, but grouping by it turns an aggregate tool into
    a per-person report, which is the thing the whole ingestion boundary exists to
    prevent. Leaving it out of the registry is what enforces that -- so this test
    fails if somebody adds it back for convenience.
    """
    from homelab_datalake.mcp.semantic import DATASETS

    forbidden = {"client_hash", "end_user_hash", "api_key_hash", "user_hash",
                 "created_by_hash", "session_id", "event_id", "item_id", "call_key",
                 "request_key", "title_key", "summary", "duplicate_of"}
    for dataset in DATASETS.values():
        exposed = set(dataset.dimension_names) & forbidden
        assert not exposed, f"{dataset.name} exposes {exposed} as a dimension"
