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


# --------------------------------------------------------------------------------
# grain='event': the rows themselves
# --------------------------------------------------------------------------------
#
# The layer could previously only aggregate. Asked for every portal opening in August
# with its exact time, it had no shape that could answer -- the finest thing it could
# say was a count in an hour bucket. These tests are about the shape being there, and
# about it refusing rather than quietly returning an aggregate when the call is
# inconsistent.


def test_records_are_returned_one_per_row_with_their_timestamps(con):
    result = run_query(con, "camera_activity", grain="event",
                       fields=["happened_at", "zone_name", "zone_state"], **AUGUST)
    assert result.grain == "event"
    assert result.columns == ["happened_at", "zone_name", "zone_state"]
    # Not folded: more rows than there are days, zones or states.
    assert result.row_count > 100
    assert all(isinstance(row["happened_at"], str) for row in result.rows)
    # Microseconds survive; an hour bucket would have been "…:00:00".
    assert any(not row["happened_at"].endswith("00:00+00:00") for row in result.rows)


def test_records_read_forwards_in_time_by_default(con):
    """Aggregates default to biggest-first; a listing of what happened does not."""
    result = run_query(con, "camera_activity", grain="event",
                       fields=["happened_at"], **AUGUST)
    stamps = [row["happened_at"] for row in result.rows]
    assert stamps == sorted(stamps)


def test_the_column_the_rows_are_sorted_by_is_always_shown(con):
    """An ordering you cannot see is an ordering you cannot check."""
    result = run_query(con, "camera_activity", grain="event",
                       fields=["zone_name"], **AUGUST)
    assert result.columns[0] == "happened_at"
    assert any("sorted by it" in note for note in result.notes)


def test_measures_are_refused_at_event_grain_rather_than_ignored(con):
    with pytest.raises(QueryError) as error:
        run_query(con, "camera_activity", grain="event", measures=["events"], **AUGUST)
    assert "do not apply at grain 'event'" in str(error.value)


def test_fields_are_refused_at_every_other_grain(con):
    with pytest.raises(QueryError) as error:
        run_query(con, "camera_activity", grain="day", fields=["happened_at"], **AUGUST)
    assert "only applies at grain 'event'" in str(error.value)


def test_an_unknown_field_names_the_real_ones(con):
    with pytest.raises(QueryError) as error:
        run_query(con, "camera_activity", grain="event", fields=["exact_time"], **AUGUST)
    message = str(error.value)
    assert "exact_time" in message and "happened_at" in message


def test_a_dimension_may_be_used_as_a_field(con):
    """The obvious export is a timestamp next to the things you would group by."""
    result = run_query(con, "camera_activity", grain="event",
                       fields=["happened_at", "camera_id", "lifecycle"], **AUGUST)
    assert {"camera_id", "lifecycle"} <= set(result.columns)


def test_filters_still_apply_to_a_listing(con):
    result = run_query(con, "camera_activity", grain="event",
                       fields=["happened_at", "zone_state"],
                       filters={"zone_state": ["open"]}, **AUGUST)
    assert result.row_count > 0
    assert {row["zone_state"] for row in result.rows} == {"open"}


def test_a_truncated_listing_says_so_in_terms_that_cannot_be_read_past(con):
    """The original bug: [count withheld] rows of 5252 came back looking complete."""
    result = run_query(con, "camera_activity", grain="event",
                       fields=["happened_at"], limit=5, **AUGUST)
    assert result.truncated
    assert result.row_count == 5
    assert any("INCOMPLETE" in note for note in result.notes)
    assert any("export_workbook" in note for note in result.notes)


def test_a_listing_defaults_to_the_ceiling_not_to_a_round_number(con):
    """Somebody asking for the rows wants the rows, not the first two hundred."""
    everything = run_query(con, "camera_activity", grain="event",
                           fields=["happened_at"], **AUGUST)
    assert everything.row_count == 250
    assert not everything.truncated


def test_a_snapshot_still_lists_one_row_per_title(con):
    """Records on a snapshot are the titles as they stood, not an aggregate."""
    result = run_query(con, "media_library", grain="event",
                       fields=["title", "resolution_class"],
                       start="2026-08-25", end="2026-08-27")
    assert result.row_count == 120  # one snapshot's worth, not three
    assert any("Snapshot of" in note for note in result.notes)


def test_the_portal_question_that_started_this_can_now_be_answered(con):
    """Every opening and closing in the window, each with its own exact time.

    This is the shape of the request that produced a workbook of daily aggregates,
    then a workbook of everything, and never the thing that was asked for.
    """
    result = run_query(
        con, "camera_activity", grain="event",
        fields=["happened_at", "zone_state", "previous_zone_state", "camera_id"],
        filters={"zone_name": ["driveway"], "state_changed": [True]},
        **AUGUST,
    )
    assert result.row_count > 1  # many per day, not one
    assert {row["zone_state"] for row in result.rows} <= {"open", "closed"}
    stamps = [row["happened_at"] for row in result.rows]
    assert len(set(stamps)) == len(stamps), "each transition has its own instant"


def test_the_first_opening_of_each_day_is_also_answerable_without_the_rows(con):
    """min() over a timestamp, so the aggregate path can answer it too."""
    result = run_query(con, "camera_activity", grain="day",
                       measures=["first_event_at", "last_event_at", "events"],
                       filters={"zone_state": ["open"]}, **AUGUST)
    assert result.row_count > 0
    first = result.rows[0]
    assert isinstance(first["first_event_at"], str)
    assert first["first_event_at"] <= first["last_event_at"]


def test_the_counts_that_reproduce_the_alerts_graph_are_measures(con):
    result = run_query(con, "camera_activity", grain="day",
                       measures=["people", "vehicles", "animals", "objects"], **AUGUST)
    assert result.row_count > 0
    assert all(row["objects"] >= row["people"] for row in result.rows)
