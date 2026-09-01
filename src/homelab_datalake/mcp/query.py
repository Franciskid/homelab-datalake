"""Turning a validated question into SQL, and running it under limits.

Every identifier in the generated statement comes from the registry in `semantic.py`;
every literal is a bound parameter. The model's strings are used only as *keys* to
look things up, and a key that is not found is an error listing what was -- which is
what lets a model correct itself instead of guessing again.

The limits are here rather than in the tool definitions because a limit a caller can
choose is not a limit. What stops a runaway query:

* **No joins.** A question addresses exactly one mart. There is no syntax for the
  model to ask for a cartesian product, so no cost estimator is needed to refuse one.
* **A row cap**, applied as LIMIT in SQL, so the rows are never materialised.
* **A wall-clock deadline**, enforced by interrupting the connection. DuckDB has no
  statement timeout, so a watchdog thread calls `interrupt()`; that is what makes the
  deadline real rather than advisory.
* **A bounded date range**, because "since the beginning of time" over the request
  fact is the one scan big enough to be worth refusing outright.
"""

from __future__ import annotations

import decimal
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime

import duckdb

from homelab_datalake.mcp.semantic import (
    ALL_GRAINS,
    EVENT_GRAIN,
    GRAINS,
    TOTAL_GRAIN,
    Dataset,
    get_dataset,
)

# Two ceilings, because the two destinations have nothing in common.
#
# MAX_ROWS bounds what a tool result carries back into the conversation, where every
# row costs context and a large listing crowds out the reasoning that should be
# reading it. MAX_EXPORT_ROWS bounds what may be written to a workbook, which nobody
# reads into context -- so the only real limit there is the spreadsheet, and holding
# an export to a conversational budget is what produced a 1000-row "complete" answer
# to a 5252-row question.
MAX_ROWS = 5_000
MAX_EXPORT_ROWS = 200_000
DEFAULT_ROWS = 200
# Records default to the ceiling rather than to a round number. A listing is asked
# for because somebody wants the rows; silently returning the first 200 of them and
# calling it the answer is the failure this whole module was rewritten to stop.
DEFAULT_RECORD_ROWS = MAX_ROWS
QUERY_TIMEOUT_SECONDS = 20.0
MAX_RANGE_DAYS = 1_100  # about three years

_PERIOD = "period"


class QueryError(ValueError):
    """A question that could not be answered as asked. The message names the options."""


@dataclass
class QueryResult:
    dataset: str
    columns: list[str]
    rows: list[dict]
    row_count: int
    truncated: bool
    sql: str
    elapsed_ms: float
    grain: str
    start: str
    end: str
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        # Order matters here, which is not a thing one usually says about a JSON
        # object. The host clips a tool result at a character budget and appends its
        # own "the result was too large -- call again with a narrower request". With
        # `notes` last, that budget ate the notes first: a listing that said
        # "COMPLETE: all 111 records are here, do not re-query narrower windows"
        # arrived as rows with the sentence missing and the host's advice attached.
        # An agent duly split one correct month-wide query into thirty day-sized
        # ones and ran out of steps.
        #
        # So everything that says how to read the answer goes before the answer. If
        # anything is lost to a cap it should be the tail of the rows, which the
        # reader can already see is a tail -- never the sentence explaining them.
        return {
            "dataset": self.dataset,
            "notes": self.notes,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "grain": self.grain,
            "range": {"start": self.start, "end": self.end},
            "columns": self.columns,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "rows": self.rows,
            # Last on purpose: it is the longest field and the least useful one to a
            # reader who has the rows.
            "sql": self.sql,
        }


def _parse_day(value: str, field_name: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise QueryError(
            f"{field_name} must be a date as YYYY-MM-DD, got {value!r}."
        ) from None


def _validated_names(requested, available: list[str], kind: str, dataset: Dataset) -> list[str]:
    out = []
    for name in requested:
        if name not in available:
            raise QueryError(
                f"{kind} {name!r} is not available on dataset {dataset.name!r}. "
                f"Available {kind}s: {sorted(available)}."
            )
        if name not in out:  # a repeat is a mistake, not a second column
            out.append(name)
    return out


def available_range(con: duckdb.DuckDBPyConnection, dataset: Dataset) -> tuple[str | None, str | None]:
    """The dates this dataset actually covers. Used to explain an empty answer."""
    row = con.execute(
        f"SELECT min({dataset.date_column}), max({dataset.date_column}) "  # noqa: S608
        f"FROM {dataset.table}"
    ).fetchone()
    lo, hi = row if row else (None, None)
    return (lo.isoformat() if lo else None, hi.isoformat() if hi else None)


def build_sql(
    dataset: Dataset,
    start: date,
    end: date,
    grain: str,
    dimensions: list[str],
    measures: list[str],
    filters: dict[str, list],
    order_by: str | None,
    descending: bool,
    limit: int,
) -> tuple[str, list]:
    """Compose the statement. Returns (sql, parameters)."""
    params: list = []
    select_parts: list[str] = []
    group_parts: list[str] = []

    if grain != TOTAL_GRAIN:
        unit = GRAINS[grain]
        # `unit` is a value from GRAINS, never the caller's string.
        # Cast back to DATE: date_trunc on a DATE returns a TIMESTAMP, and a bucket
        # rendered as "2026-08-03 00:00:00" invites a reader to think it means an
        # instant rather than a week.
        select_parts.append(
            f"date_trunc('{unit}', {dataset.date_column})::DATE AS {_PERIOD}"
        )
        group_parts.append(_PERIOD)

    for name in dimensions:
        dim = dataset.dimension(name)
        select_parts.append(f'{dim.sql} AS "{name}"')
        group_parts.append(f'"{name}"')

    for name in measures:
        select_parts.append(f'{dataset.measure(name).sql} AS "{name}"')

    where = [f"{dataset.date_column} BETWEEN ? AND ?"]
    params.extend([start, end])

    for name, values in filters.items():
        dim = dataset.dimension(name)
        literals = [v for v in values if v is not None]
        wants_null = len(literals) != len(values)
        clause = []
        if literals:
            placeholders = ", ".join("?" for _ in literals)
            clause.append(f"{dim.sql} IN ({placeholders})")
            params.extend(literals)
        if wants_null:
            clause.append(f"{dim.sql} IS NULL")
        where.append("(" + " OR ".join(clause) + ")")

    sql = f"SELECT {', '.join(select_parts)} FROM {dataset.table}"  # noqa: S608
    sql += " WHERE " + " AND ".join(where)
    if group_parts:
        sql += " GROUP BY " + ", ".join(group_parts)

    if order_by:
        direction = "DESC" if descending else "ASC"
        # order_by has already been validated against the projected column names.
        column = _PERIOD if order_by == _PERIOD else f'"{order_by}"'
        sql += f" ORDER BY {column} {direction} NULLS LAST"
    elif grain != TOTAL_GRAIN:
        sql += f" ORDER BY {_PERIOD} ASC"

    # One more than asked, so truncation is detected rather than guessed at.
    sql += f" LIMIT {int(limit) + 1}"
    return sql, params


def _where_clause(dataset: Dataset, start: date, end: date, filters: dict[str, list]) -> tuple[str, list]:
    """The date range and the dimension filters, shared by both query shapes.

    Filters are dimensions only, in records as in aggregates. A dimension is by
    construction something with few enough distinct values to be worth naming, which
    is exactly the property that makes a filter useful; attributes are unique per row
    and filtering on one is a lookup by primary key wearing a filter's clothes.
    """
    params: list = [start, end]
    where = [f"{dataset.date_column} BETWEEN ? AND ?"]
    for name, values in filters.items():
        dim = dataset.dimension(name)
        literals = [v for v in values if v is not None]
        wants_null = len(literals) != len(values)
        clause = []
        if literals:
            placeholders = ", ".join("?" for _ in literals)
            clause.append(f"{dim.sql} IN ({placeholders})")
            params.extend(literals)
        if wants_null:
            clause.append(f"{dim.sql} IS NULL")
        where.append("(" + " OR ".join(clause) + ")")
    return " AND ".join(where), params


def build_records_sql(
    dataset: Dataset,
    start: date,
    end: date,
    fields: list[str],
    filters: dict[str, list],
    order_by: str | None,
    descending: bool,
    limit: int,
) -> tuple[str, list]:
    """One row per record: no GROUP BY, no aggregate, nothing collapsed.

    Everything that makes the aggregate builder safe still holds here. Identifiers
    come from the registry, literals are bound, there is one table and no join. The
    only thing that changes is that the rows are not folded together -- which is what
    a question like "every time the gate opened, with the time" actually asks for,
    and what the layer previously had no way to express at all.
    """
    select_parts = [f'{dataset.field(name).sql} AS "{name}"' for name in fields]
    clause, params = _where_clause(dataset, start, end, filters)
    sql = f"SELECT {', '.join(select_parts)} FROM {dataset.table} WHERE {clause}"  # noqa: S608
    if order_by:
        direction = "DESC" if descending else "ASC"
        sql += f' ORDER BY "{order_by}" {direction} NULLS LAST'
    sql += f" LIMIT {int(limit) + 1}"
    return sql, params


def _execute_bounded(
    con: duckdb.DuckDBPyConnection, sql: str, params: list, timeout: float
) -> tuple[list[str], list[tuple]]:
    """Run the statement, interrupting it if it outlives the deadline.

    DuckDB has no statement timeout, so this is a watchdog: a timer that calls
    interrupt() on the connection. Without it a deadline would be a comment.
    """
    timer = threading.Timer(timeout, con.interrupt)
    timer.start()
    try:
        cursor = con.execute(sql, params)
        columns = [d[0] for d in cursor.description]
        return columns, cursor.fetchall()
    except duckdb.InterruptException:
        raise QueryError(
            f"query exceeded the {timeout:g}s limit and was cancelled. Narrow the "
            "date range, or ask for fewer dimensions."
        ) from None
    finally:
        timer.cancel()


def run_query(
    con: duckdb.DuckDBPyConnection,
    dataset_name: str,
    start: str,
    end: str,
    grain: str = "day",
    dimensions: list[str] | None = None,
    measures: list[str] | None = None,
    fields: list[str] | None = None,
    filters: dict[str, list] | None = None,
    order_by: str | None = None,
    descending: bool | None = None,
    limit: int | None = None,
    timeout: float = QUERY_TIMEOUT_SECONDS,
    ceiling: int = MAX_ROWS,
) -> QueryResult:
    dataset = get_dataset(dataset_name)
    records = grain == EVENT_GRAIN
    dimensions = list(dimensions or [])
    fields = list(fields or [])
    filters = dict(filters or {})
    notes: list[str] = []

    if grain not in ALL_GRAINS:
        raise QueryError(f"unknown grain {grain!r}. Available: {ALL_GRAINS}.")

    if records and not dataset.attributes:
        raise QueryError(
            f"{dataset.name!r} cannot be queried at grain 'event': it declares no "
            f"row-level fields. Use one of {sorted([*GRAINS, TOTAL_GRAIN])}."
        )

    # Measures are aggregates. Asking for one alongside a row-level listing is a
    # request for two different shapes at once, and quietly ignoring it would return
    # a listing that is missing a column the caller believes it asked for.
    if records:
        if measures:
            raise QueryError(
                "measures do not apply at grain 'event', which returns the rows "
                f"themselves rather than aggregating them. Asked for: {sorted(measures)}. "
                "Drop them and name `fields` instead, or use a different grain."
            )
        measures = []
    else:
        if fields:
            raise QueryError(
                "`fields` only applies at grain 'event'. At any other grain the rows "
                "are aggregated, so there is no single value to project -- ask for "
                "dimensions and measures instead, or set grain='event'."
            )
        measures = list(measures if measures is not None else dataset.default_measures)

    start_day = _parse_day(start, "start")
    end_day = _parse_day(end, "end")
    if end_day < start_day:
        raise QueryError(f"end ({end}) is before start ({start}).")
    span = (end_day - start_day).days + 1
    if span > MAX_RANGE_DAYS:
        raise QueryError(
            f"range of {span} days exceeds the {MAX_RANGE_DAYS}-day maximum. "
            "Ask for a narrower window, or use a coarser grain over a shorter one."
        )

    dimensions = _validated_names(dimensions, dataset.dimension_names, "dimension", dataset)
    measures = _validated_names(measures, dataset.measure_names, "measure", dataset)
    if records:
        fields = _validated_names(
            fields or list(dataset.default_fields), dataset.field_names, "field", dataset
        )
        if not fields:
            fields = list(dataset.field_names)
    elif not measures:
        raise QueryError(
            f"at least one measure is required. Available: {sorted(dataset.measure_names)}."
        )

    for name in filters:
        if name not in dataset.dimension_names:
            raise QueryError(
                f"cannot filter on {name!r}: it is not a dimension of {dataset.name!r}. "
                f"Available dimensions: {sorted(dataset.dimension_names)}."
            )
    filters = {k: (v if isinstance(v, list) else [v]) for k, v in filters.items()}
    for name, values in filters.items():
        if not values:
            raise QueryError(f"filter on {name!r} has no values; drop it instead.")

    # A snapshot dataset describes a moment, not an interval. Bucketing it by day and
    # summing would count the same film once per day it sat on the disk -- a number
    # that looks like growth and is arithmetic. So the range selects *which* snapshot
    # to read, and the answer is that one day.
    if dataset.kind == "snapshot":
        chosen = con.execute(
            f"SELECT max({dataset.date_column}) FROM {dataset.table} "  # noqa: S608
            f"WHERE {dataset.date_column} BETWEEN ? AND ?",
            [start_day, end_day],
        ).fetchone()[0]
        if chosen is None:
            lo, hi = available_range(con, dataset)
            return QueryResult(
                dataset=dataset.name, columns=[], rows=[], row_count=0, truncated=False,
                sql="", elapsed_ms=0.0, grain=TOTAL_GRAIN, start=start, end=end,
                notes=[
                    f"No snapshot of {dataset.name} exists between {start} and {end}.",
                    f"Snapshots are available from {lo} to {hi}." if lo else
                    "This dataset has no snapshots at all yet.",
                    (
                        "Do not describe this as a change or a decline: it means no "
                        "snapshot was taken in that window, not that the library was empty."
                    ),
                ],
            )
        # Records are still records on a snapshot -- one row per title as it stood on
        # that day -- so only the bucketing grains collapse to a single snapshot.
        if grain not in (TOTAL_GRAIN, EVENT_GRAIN):
            notes.append(
                f"{dataset.name} is a snapshot, so grain does not apply: this is the "
                f"library as it stood on {chosen.isoformat()}, the latest snapshot in "
                "the requested range."
            )
            grain = TOTAL_GRAIN
        else:
            notes.append(f"Snapshot of {chosen.isoformat()}.")
        start_day = end_day = chosen

    if records:
        # An ordering you cannot see is an ordering you cannot check, so the column
        # the listing is sorted by is always projected.
        if order_by is None:
            order_by = dataset.record_order
        if order_by and order_by not in fields:
            if order_by in dataset.field_names:
                fields = [order_by, *fields]
                notes.append(f"Added {order_by!r} to the columns, because the rows are sorted by it.")
            else:
                raise QueryError(
                    f"cannot order by {order_by!r}: it is not a field of "
                    f"{dataset.name!r}. Available fields: {sorted(dataset.field_names)}."
                )
        if descending is None:
            # Chronological. A listing of things that happened reads forwards, and the
            # aggregate default of newest-first would put the last day of the month at
            # the top of an export nobody asked to reverse.
            descending = False
        projected = list(fields)
    else:
        descending = True if descending is None else descending
        projected = ([_PERIOD] if grain != TOTAL_GRAIN else []) + dimensions + measures
        if order_by and order_by not in projected:
            raise QueryError(
                f"cannot order by {order_by!r}: it is not among the selected columns "
                f"{projected}. Order by a measure or dimension you asked for."
            )
        if order_by is None and grain == TOTAL_GRAIN:
            order_by = measures[0]

    default_limit = DEFAULT_RECORD_ROWS if records else DEFAULT_ROWS
    ceiling = max(1, int(ceiling))
    limit = max(1, min(int(default_limit if limit is None else limit), ceiling))

    if records:
        sql, params = build_records_sql(
            dataset, start_day, end_day, fields, filters, order_by, descending, limit,
        )
    else:
        sql, params = build_sql(
            dataset, start_day, end_day, grain, dimensions, measures,
            filters, order_by, descending, limit,
        )

    began = time.perf_counter()
    columns, raw = _execute_bounded(con, sql, params, timeout)
    elapsed = (time.perf_counter() - began) * 1000

    truncated = len(raw) > limit
    if records and not truncated and raw:
        # The other half of the truncation note, and it earns its place.
        #
        # A cut-off listing shouts; a complete one used to say nothing, and an agent
        # reading "[count withheld] rows, cap 5000" has to *infer* that it has them all. One did
        # not: it made the correct whole-month query, then re-queried in weeks, then
        # in days, thirty calls in all, and ran out of steps before writing anything.
        # Nothing was wrong with the data. The result simply never said it was
        # finished, and silence is not an answer a careful reader can act on.
        notes.append(
            f"COMPLETE: all {len(raw)} matching records are here, and the cap of "
            f"{limit} was not reached. Do not re-query narrower windows to check -- "
            "this is the whole set, and splitting it up returns the same rows again."
        )
    if truncated:
        raw = raw[:limit]
        if records:
            # Said in the strongest terms available, because this is the exact shape
            # of the bug that started the rewrite: a listing cut short reads as a
            # complete listing, and the reader has no way to tell from the rows.
            notes.append(
                f"INCOMPLETE: cut off at {limit} rows, and there are more. Do NOT "
                "describe this as every matching record, and do not summarise it as "
                "though it were. Narrow the range or add a filter, or use "
                "export_workbook, which writes to a file and allows far more rows."
            )
        else:
            notes.append(
                f"Truncated to {limit} rows. This is not the whole answer -- narrow the "
                "range, filter, or use a coarser grain before drawing a conclusion from it."
            )

    rows = [dict(zip(columns, map(_jsonable, r), strict=True)) for r in raw]

    # An empty answer is a real answer, and the most likely way a model goes wrong
    # here is narrating a confident story over nothing. So say plainly that there is
    # no data, and say what range does have some -- which distinguishes "quiet
    # period" from "wrong window", the two the model cannot otherwise tell apart.
    if not rows:
        lo, hi = available_range(con, dataset)
        notes.append("No rows matched. This is an empty result, not a zero.")
        if lo and hi:
            notes.append(f"{dataset.name} holds data from {lo} to {hi}.")
            if hi < start or lo > end:
                notes.append(
                    "The requested range lies entirely outside that, so the window is "
                    "the likely mistake."
                )
        else:
            notes.append(f"{dataset.name} is empty: no partitions have landed yet.")
        if filters:
            notes.append(
                f"Filters applied: {filters}. One of these may not match any value; "
                "call describe_dataset to see what values exist."
            )
        notes.append("Do not summarise or estimate figures for this period.")

    return QueryResult(
        dataset=dataset.name,
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        sql=_tidy(sql),
        elapsed_ms=elapsed,
        grain=grain,
        start=start_day.isoformat(),
        end=end_day.isoformat(),
        notes=notes,
    )


def _jsonable(value):
    """Types DuckDB returns that JSON does not carry.

    DECIMAL becomes float: these are display figures already rounded in the measure
    definition, not ledger arithmetic, and at homelab magnitudes a float64 carries
    six decimal places exactly. Dates and timestamps become ISO strings so the
    caller reads a date rather than a repr.
    """
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _tidy(sql: str) -> str:
    """The generated SQL travels back with the answer, so it can be checked."""
    return re.sub(r"\s+", " ", sql).strip()
