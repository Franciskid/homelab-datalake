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

from homelab_datalake.mcp.semantic import GRAINS, TOTAL_GRAIN, Dataset, get_dataset

MAX_ROWS = 5_000
DEFAULT_ROWS = 200
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
        return {
            "dataset": self.dataset,
            "columns": self.columns,
            "rows": self.rows,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "grain": self.grain,
            "range": {"start": self.start, "end": self.end},
            "sql": self.sql,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "notes": self.notes,
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
    filters: dict[str, list] | None = None,
    order_by: str | None = None,
    descending: bool = True,
    limit: int = DEFAULT_ROWS,
    timeout: float = QUERY_TIMEOUT_SECONDS,
) -> QueryResult:
    dataset = get_dataset(dataset_name)
    dimensions = list(dimensions or [])
    measures = list(measures or dataset.default_measures)
    filters = dict(filters or {})
    notes: list[str] = []

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
    if not measures:
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

    if grain not in GRAINS and grain != TOTAL_GRAIN:
        raise QueryError(
            f"unknown grain {grain!r}. Available: {sorted([*GRAINS, TOTAL_GRAIN])}."
        )

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
        if grain != TOTAL_GRAIN:
            notes.append(
                f"{dataset.name} is a snapshot, so grain does not apply: this is the "
                f"library as it stood on {chosen.isoformat()}, the latest snapshot in "
                "the requested range."
            )
        else:
            notes.append(f"Snapshot of {chosen.isoformat()}.")
        grain = TOTAL_GRAIN
        start_day = end_day = chosen

    projected = ([_PERIOD] if grain != TOTAL_GRAIN else []) + dimensions + measures
    if order_by and order_by not in projected:
        raise QueryError(
            f"cannot order by {order_by!r}: it is not among the selected columns "
            f"{projected}. Order by a measure or dimension you asked for."
        )
    if order_by is None and grain == TOTAL_GRAIN:
        order_by = measures[0]

    limit = max(1, min(int(limit), MAX_ROWS))

    sql, params = build_sql(
        dataset, start_day, end_day, grain, dimensions, measures,
        filters, order_by, descending, limit,
    )

    began = time.perf_counter()
    columns, raw = _execute_bounded(con, sql, params, timeout)
    elapsed = (time.perf_counter() - began) * 1000

    truncated = len(raw) > limit
    if truncated:
        raw = raw[:limit]
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
