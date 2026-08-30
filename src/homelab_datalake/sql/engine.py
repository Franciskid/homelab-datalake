"""Running a person's SQL against the lake, safely.

The console is behind Keycloak, so the person on the other end is known. That is a
reason to give them the data, not a reason to give them the process: a mistyped join
should cost one browser tab, not the nightly build. So the guarantees here are about
blast radius rather than about trust.

**Nothing can be written.** The serving copy is attached READ_ONLY, and the statement
is parsed by DuckDB itself -- not matched against a pattern -- so only SELECT and
EXPLAIN reach the engine. `COPY ... TO`, ATTACH, INSTALL and SET are refused before
execution, which closes the usual way out of a read-only database.

**Nothing can reach the filesystem.** After the views are built, LocalFileSystem is
disabled and the configuration is locked, so a query cannot read `/etc/passwd` or
write anywhere, and cannot turn either back on.

**Nothing can run forever.** Every query gets a deadline, and DuckDB is interrupted
when it passes. Every result is capped. A cross join typed by accident returns an
error in a few seconds instead of filling memory.

The connection is built per request. That costs a few milliseconds and means one
person's session can never affect another's.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from homelab_datalake import layout, serving
from homelab_datalake.resources import LakeStorage

# Rows returned to the browser. One more than this is fetched, so the page can say
# the result was truncated rather than quietly showing the first thousand.
MAX_ROWS = 1_000

# Seconds a query may run. Long enough for a full scan of the largest mart, short
# enough that a mistake does not hold a worker.
TIMEOUT_SECONDS = 30

# DuckDB's own statement classification. Anything not here is refused with its real
# name, so "why was this rejected" has an answer.
ALLOWED_STATEMENTS = frozenset({"SELECT", "EXPLAIN"})


class QueryRefused(Exception):
    """The statement was rejected before it reached the engine."""


@dataclass
class Result:
    columns: list[str] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    truncated: bool = False
    elapsed_ms: int = 0


def statement_types(con: duckdb.DuckDBPyConnection, sql: str) -> list[str]:
    """What DuckDB thinks each statement is. Raises on a syntax error."""
    return [str(s.type).rsplit(".", 1)[-1] for s in con.extract_statements(sql)]


def check(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    if not sql.strip():
        raise QueryRefused("nothing to run")
    try:
        kinds = statement_types(con, sql)
    except duckdb.ParserException as exc:
        raise QueryRefused(str(exc)) from None
    if len(kinds) != 1:
        raise QueryRefused(
            f"{len(kinds)} statements were given; run one at a time so the row cap "
            "and the timeout apply to the thing you are looking at"
        )
    if kinds[0] not in ALLOWED_STATEMENTS:
        raise QueryRefused(
            f"{kinds[0]} is not allowed here. This console is read-only: "
            f"{' and '.join(sorted(ALLOWED_STATEMENTS))} only."
        )


def build_views(con: duckdb.DuckDBPyConnection, lake: LakeStorage) -> list[str]:
    """Give the connection one schema per lake layer, then close the door.

    `marts` comes from the published serving copy rather than from Parquet: it is
    the same rows, and reading a local file is faster than a thousand range requests
    to MinIO for a query somebody is waiting on. `raw` and `curated` are views over
    the objects, which is what makes the layout visible -- a person can see that
    `curated.int_npm_requests` is a directory in the bucket and go read it with
    something else.

    A layer with nothing landed yet is skipped rather than created broken, so a
    fresh deployment shows fewer tables instead of a page of errors.
    """
    built = []
    lake.configure_duckdb(con)
    for layer in (layout.LAYER_RAW, layout.LAYER_CURATED):
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{layer}"')
        for dataset in layout.datasets_in(layer):
            target = lake.duckdb_scan_target(dataset.prefix).replace("'", "''")
            try:
                con.execute(
                    f'CREATE OR REPLACE VIEW "{layer}"."{dataset.table}" AS '  # noqa: S608
                    f"SELECT * FROM read_parquet('{target}', union_by_name=true, "
                    "hive_partitioning=true)"
                )
                built.append(f"{layer}.{dataset.table}")
            except duckdb.Error:
                # Nothing landed under that prefix yet. Not an error: it is what a
                # source that has never run looks like.
                continue
    return built


def connect(serving_path: str | None = None, lake: LakeStorage | None = None):
    """A locked-down connection with `marts`, `curated` and `raw` in scope."""
    lake = lake or LakeStorage.from_env()
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC';")

    path = serving_path or serving.SERVING_PATH
    con.execute(f"ATTACH '{path.replace(chr(39), chr(39) * 2)}' AS published (READ_ONLY);")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    for (table,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'published'"
    ).fetchall():
        con.execute(
            f'CREATE OR REPLACE VIEW marts."{table}" AS '  # noqa: S608
            f'SELECT * FROM published.marts."{table}"'
        )

    build_views(con, lake)

    _seal(con, lake, path)
    return con


def _seal(con: duckdb.DuckDBPyConnection, lake: LakeStorage, serving_path: str) -> None:
    """Close the door on the filesystem, then throw away the key.

    Two branches, because the lake is in a different place in the two deployments
    and a single setting cannot be right for both.

    In production the lake is on MinIO. Once the serving copy is attached nothing
    local is needed, so the local filesystem is switched off outright and httpfs --
    which is a different filesystem to DuckDB -- keeps working.

    In development and in CI the lake *is* a local directory. Switching the local
    filesystem off there would leave `curated.*` and `raw.*` unreadable: the guard
    would have made the console useless rather than safe. So external access is
    disabled and the two directories that hold the data are allowlisted back, which
    refuses `/etc/passwd` just the same.

    The order is not free. `allowed_directories` cannot be changed once external
    access is off, and external access cannot be turned back on while the database
    runs -- so the allowlist is written first and the door is shut after it.
    `lock_configuration` is what makes either branch hold: without it the first thing
    a query would do is set it back.
    """
    if lake.uses_s3:
        con.execute("SET disabled_filesystems='LocalFileSystem';")
    else:
        allowed = sorted({
            str(Path(lake.local_root).resolve()),
            str(Path(serving_path).resolve().parent),
        })
        listed = ", ".join("'" + d.replace("'", "''") + "'" for d in allowed)
        con.execute(f"SET allowed_directories=[{listed}];")
        con.execute("SET enable_external_access=false;")
    con.execute("SET lock_configuration=true;")


def run(con: duckdb.DuckDBPyConnection, sql: str, max_rows: int = MAX_ROWS,
        timeout_seconds: int = TIMEOUT_SECONDS) -> Result:
    """Execute one checked statement, capped and on a deadline."""
    check(con, sql)

    # The cap is applied by wrapping rather than by fetching fewer rows, so DuckDB
    # can stop early instead of computing a million rows and being handed a thousand.
    # EXPLAIN output is a couple of rows and is not a relation, so it is run as-is.
    wrapped = sql if statement_types(con, sql) == ["EXPLAIN"] else (
        # The statement is the point. check() has already had DuckDB parse it and
        # refused anything that is not one SELECT, which is a stronger guarantee
        # than the pattern S608 looks for.
        f"SELECT * FROM ({sql.rstrip().rstrip(';')}) LIMIT {max_rows + 1}"  # noqa: S608
    )

    expired = threading.Event()
    timer = threading.Timer(timeout_seconds, lambda: (expired.set(), con.interrupt()))
    timer.start()
    started = time.monotonic()
    try:
        cursor = con.execute(wrapped)
        columns = [d[0] for d in cursor.description or []]
        rows = cursor.fetchall()
    except duckdb.InterruptException:
        raise QueryRefused(
            f"the query was still running after {timeout_seconds}s and was stopped. "
            "Add a filter on the partition column, or aggregate before joining."
        ) from None
    except duckdb.Error as exc:
        if expired.is_set():
            raise QueryRefused(f"stopped after {timeout_seconds}s") from None
        raise QueryRefused(str(exc)) from None
    finally:
        timer.cancel()

    elapsed = int((time.monotonic() - started) * 1000)
    truncated = len(rows) > max_rows
    return Result(
        columns=columns,
        rows=[list(r) for r in rows[:max_rows]],
        truncated=truncated,
        elapsed_ms=elapsed,
    )


def catalogue(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict]]:
    """Every table the console can see, with its columns, grouped by layer.

    Read from DuckDB's catalogue rather than from the registry, so the sidebar shows
    what is really there. A dataset whose partitions have not landed is absent, which
    is the truth a person needs before they write a query against it.
    """
    grouped: dict[str, list[dict]] = {}
    for layer in ("marts", layout.LAYER_CURATED, layout.LAYER_RAW):
        rows = con.execute(
            "SELECT table_name, list(column_name ORDER BY column_index) "
            "FROM duckdb_columns() WHERE schema_name = ? AND database_name = 'memory' "
            "GROUP BY 1 ORDER BY 1",
            [layer],
        ).fetchall()
        if rows:
            grouped[layer] = [{"table": t, "columns": c} for t, c in rows]
    return grouped
