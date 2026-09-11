"""Refusing a comparison against a value the column never holds.

This is the guard for statements the semantic layer never sees. The platform's Python
workbench relays raw SQL straight to DuckDB, and on 11 Sep 2026 it relayed
`direction = 'in'` against a column holding 'entrée' and 'sortie'. Valid SQL, existing
column, well-typed literal, zero rows, workbook emailed. There is no layer below this
one that could have objected, so these tests are about this one objecting.

The parsing tests matter more than they look. The mistake was not in the WHERE clause
-- it was inside a CASE WHEN in the select list -- so a guard that reads only filters
would have watched the one place the bug was not.

The last section runs the whole thing: a real lake, a real session, a real statement,
refused. The parts can each be right while the wiring between them is not, and the
wiring is what a report reaching an inbox depends on.
"""

from datetime import date

import duckdb
import pyarrow as pa
import pytest

from homelab_datalake import layout
from homelab_datalake.api import query as q
from homelab_datalake.api.query import enum_columns, enum_refusal, literal_comparisons
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def parser():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def _pairs(parser, sql):
    return set(literal_comparisons(parser, sql))


# --- what the parser finds ----------------------------------------------------


def test_a_comparison_in_the_where_clause_is_found(parser):
    assert ("direction", "in") in _pairs(
        parser, "SELECT 1 FROM marts.fct_camera_event WHERE direction = 'in'"
    )


def test_a_comparison_inside_a_case_in_the_select_list_is_found(parser):
    """The exact shape that produced a week of zeroes."""
    sql = (
        "SELECT sum(CASE WHEN direction = 'in' THEN count_person ELSE 0 END) AS people_in "
        "FROM marts.fct_camera_event"
    )
    assert ("direction", "in") in _pairs(parser, sql)


def test_an_in_list_is_found_value_by_value(parser):
    found = _pairs(parser, "SELECT 1 FROM t WHERE direction IN ('in', 'out')")
    assert ("direction", "in") in found
    assert ("direction", "out") in found


def test_a_negated_comparison_is_found_too(parser):
    """`<> 'in'` is always true against a column that never holds it -- same mistake."""
    assert ("direction", "in") in _pairs(parser, "SELECT 1 FROM t WHERE direction <> 'in'")


def test_a_qualified_column_is_the_same_column(parser):
    assert ("direction", "in") in _pairs(
        parser, "SELECT 1 FROM marts.fct_camera_event e WHERE e.direction = 'in'"
    )


def test_the_literal_may_be_written_on_either_side(parser):
    assert ("direction", "in") in _pairs(parser, "SELECT 1 FROM t WHERE 'in' = direction")


def test_non_string_and_null_comparisons_are_ignored(parser):
    """Only string literals. A number or a NULL is not a misspelt enum value."""
    found = _pairs(parser, "SELECT 1 FROM t WHERE status = 200 AND direction IS NULL")
    assert found == set()


def test_a_statement_that_cannot_be_serialised_yields_nothing_rather_than_raising(parser):
    """A guard that turns an unusual statement into an error is worse than the silence."""
    assert literal_comparisons(parser, "NOT SQL AT ALL ((") == []


# --- what the guard refuses ---------------------------------------------------


def _known(values):
    return lambda column: values


def test_a_value_the_column_never_holds_is_refused_with_the_real_values():
    message = enum_refusal([("direction", "in")], _known(["entrée", "sortie"]))
    assert message is not None
    assert "'in'" in message
    assert "entrée" in message and "sortie" in message


def test_a_value_the_column_does_hold_passes():
    assert enum_refusal([("direction", "entrée")], _known(["entrée", "sortie"])) is None


def test_a_column_with_too_many_values_to_enumerate_is_not_judged():
    """None means do not judge: refusing because we could not check would block work."""
    assert enum_refusal([("direction", "in")], _known(None)) is None


def test_a_column_the_semantic_layer_does_not_describe_is_not_judged():
    assert enum_refusal([("not_a_column", "whatever")], _known(["a"])) is None


def test_null_among_the_known_values_is_reported_as_such():
    message = enum_refusal([("direction", "in")], _known(["entrée", None]))
    assert message is not None and "null" in message


def test_the_refusal_names_the_first_offending_value_only():
    """One clear reason to fix beats a list the reader has to triage."""
    message = enum_refusal(
        [("direction", "in"), ("direction", "out")], _known(["entrée", "sortie"])
    )
    assert message is not None and message.count("never holds") == 1


def test_a_boolean_column_compared_against_a_string_is_not_judged():
    """`state_changed = 'true'` is coerced by DuckDB and is how callers write it.

    Judging it against the values True and False would refuse a correct statement as
    a misspelling, which is this guard inventing a mistake rather than catching one.
    """
    assert enum_refusal([("state_changed", "true")], _known([True, False])) is None


def test_a_numeric_column_compared_against_a_string_is_not_judged():
    assert enum_refusal([("started_hour_of_day", "7")], _known([7, 8, 9])) is None


def test_a_string_column_is_still_judged_among_columns_that_are_not():
    """The narrowing must not cost the case the guard was written for."""
    assert enum_refusal([("direction", "in")], _known(["entrée", "sortie"])) is not None


# --- which columns are guarded at all -----------------------------------------


def test_direction_is_a_guarded_column_and_points_at_the_camera_mart():
    columns = enum_columns()
    assert "direction" in columns
    assert any("fct_camera_event" in table for table in columns["direction"])


def test_a_dimension_that_is_an_expression_is_not_guarded():
    """Only bare columns. A computed dimension's value is not the column's own."""
    for column in enum_columns():
        assert column.isidentifier()


# --------------------------------------------------------------------------------
# end to end: a real lake, a real session, a real refusal
# --------------------------------------------------------------------------------


@pytest.fixture
def camera_lake(tmp_path) -> LakeStorage:
    """A lake holding the one mart this guard has an opinion about.

    Built through layout.prefix rather than a written-out path: the camera mart lives
    under a different domain than the traffic one, and a guessed prefix would mount
    nothing and prove nothing while still passing.
    """
    storage = LakeStorage(local_root=str(tmp_path / "lake"))
    storage.write_table(
        f"{layout.prefix('fct_camera_event')}/partition_date=2026-09-08/data.parquet",
        pa.table({
            "partition_date": pa.array([date(2026, 9, 8)] * 4, pa.date32()),
            "direction": pa.array(["entrée", "sortie", "entrée", None]),
            "camera_id": pa.array(["cam-0"] * 4),
            "count_person": pa.array([1, 0, 2, 0]),
        }),
    )
    return storage


def _first_value(result):
    row = result.rows[0]
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


def test_the_report_that_started_this_would_now_be_refused(camera_lake):
    """`direction = 'in'` against a column holding 'entrée'. The whole incident."""
    with q.LakeSession(lake=camera_lake) as session, pytest.raises(q.QueryRefused) as raised:
        q.run(session, "SELECT count(*) FROM marts.fct_camera_event WHERE direction = 'in'")
    message = str(raised.value)
    assert "never holds" in message
    assert "entrée" in message, "the refusal has to carry the value that would have worked"


def test_the_case_when_shape_is_refused_too(camera_lake):
    """Not in the WHERE clause. This is where the bad report's mistake actually sat."""
    sql = (
        "SELECT sum(CASE WHEN direction = 'in' THEN count_person ELSE 0 END) AS people_in "
        "FROM marts.fct_camera_event"
    )
    with q.LakeSession(lake=camera_lake) as session, pytest.raises(q.QueryRefused):
        q.run(session, sql)


def test_the_same_statement_with_the_real_value_answers(camera_lake):
    """The guard must refuse the mistake and nothing else."""
    sql = (
        "SELECT sum(CASE WHEN direction = 'entrée' THEN count_person ELSE 0 END) AS people_in "
        "FROM marts.fct_camera_event"
    )
    with q.LakeSession(lake=camera_lake) as session:
        result = q.run(session, sql)
    assert result.row_count == 1
    assert _first_value(result) == 3


def test_a_statement_touching_no_guarded_value_is_untouched(camera_lake):
    with q.LakeSession(lake=camera_lake) as session:
        result = q.run(session, "SELECT count(*) FROM marts.fct_camera_event")
    assert _first_value(result) == 4


def test_the_known_values_are_read_once_and_remembered(camera_lake):
    """The session caches them; it is discarded when the lake is republished."""
    with q.LakeSession(lake=camera_lake) as session:
        first = session.enum_values("direction")
        assert sorted(v for v in first if v is not None) == ["entrée", "sortie"]
        assert session.enum_values("direction") is first  # the same object, not a re-read
