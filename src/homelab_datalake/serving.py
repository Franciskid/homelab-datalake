"""The serving copy: what the MCP layer reads.

DuckDB allows one writing process, and -- measured, not assumed -- a *read-only*
open fails too while that lock is held:

    IO Error: Could not set lock on file ".../datalake.duckdb"

So a reader pointed at the warehouse file would not merely see stale data during the
nightly build, it would raise for the length of it. Retrying would turn that into
latency instead of errors, which is not better.

The fix is to separate building from serving. The build writes `datalake.duckdb`;
when it finishes, the marts are copied into `serving.duckdb` and that file is swapped
in atomically. Readers never contend with the build, and a build that fails never
reaches readers -- they keep the previous good copy rather than seeing half a rebuild.

Two things fall out of this that are worth stating, because they were the reason for
choosing a copy over a lock dance:

* **Only the marts are copied.** Not raw, not staging. The serving file physically
  cannot leak a column the semantic layer does not expose, because the column is not
  in the file. That is a stronger guarantee than a permission on a view.
* **The copy is a consistent snapshot**, taken through the engine rather than by
  copying bytes underneath a writer.

It costs a file copy of a few tens of megabytes a night, which at this size is not a
cost worth optimising.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb

from homelab_datalake.mcp.semantic import create_dataset_views, validate

logger = logging.getLogger(__name__)

# Absolute, for the same reason DUCKDB_PATH is: this file is opened by the Dagster
# asset that publishes it and by the MCP server that reads it, and those two do not
# share a working directory.
SERVING_PATH = str(Path(os.environ.get("DUCKDB_SERVING_PATH", "warehouse/serving.duckdb")).resolve())

# dbt-duckdb prefixes the configured schema with the target's, so `marts` becomes
# `main_marts`. Matching on the suffix means a target rename does not silently
# publish nothing -- and the assertion in publish() turns "nothing matched" into a
# failure rather than an empty file that looks successful.
MART_SCHEMA_SUFFIX = "marts"

# The alias the serving file is attached under. Queries still name `marts.<table>`,
# so the same SQL works against an attached copy and against a plain file -- which is
# what lets the tests run on a database built directly rather than published.
SERVING_ALIAS = "serving"


@dataclass
class Published:
    tables: list[str]
    rows: int
    bytes_written: int
    path: str


def mart_tables(con: duckdb.DuckDBPyConnection, database: str | None = None) -> list[tuple[str, str]]:
    """Every table in a marts schema, as (schema, table).

    The schema match is done here rather than in a LIKE pattern: `main_marts`
    contains an underscore, which is LIKE's single-character wildcard, and DuckDB
    treats a backslash as a literal unless an ESCAPE clause is given. The first
    version of this silently matched nothing -- caught only because publishing an
    empty serving database is an error rather than a quiet success.

    `database` restricts the search to one attached catalogue. publish() needs that:
    it has the warehouse and the copy it is building attached at the same time, and
    without it the marts it has just written would be found again as sources.
    """
    query = "SELECT schema_name, table_name FROM duckdb_tables()"
    parameters: list = []
    if database is not None:
        query += " WHERE database_name = ?"
        parameters.append(database)
    rows = con.execute(query + " ORDER BY table_name", parameters).fetchall()
    return [
        (schema, table)
        for schema, table in rows
        if schema == MART_SCHEMA_SUFFIX or schema.endswith(f"_{MART_SCHEMA_SUFFIX}")
    ]


def publish(source_path: str, serving_path: str = SERVING_PATH) -> Published:
    """Copy the marts out of the warehouse into a fresh serving file, atomically.

    Written to a temporary name and renamed into place, so a reader opening the file
    at any instant gets either the whole previous copy or the whole new one. A
    half-written serving database is never observable.
    """
    target = Path(serving_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    previous = _row_counts(target)
    staged = target.with_suffix(target.suffix + ".building")
    for leftover in (staged, Path(str(staged) + ".wal")):
        leftover.unlink(missing_ok=True)

    # The warehouse is attached READ_ONLY onto an in-memory root rather than opened
    # directly, and that is not a stylistic choice. Opening it read-write takes
    # DuckDB's single write lock, which made this step and the two that publish the
    # lake layers race for the same file -- whichever started second failed with
    # "Conflicting lock is held". Nothing here writes to the warehouse, so nothing
    # here should hold that lock; several read-only handles coexist happily.
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET TimeZone='UTC';")
        # Attached under the file's own name, not a readable alias. DuckDB stores a
        # view's SQL with its catalogue qualified, so anything in the warehouse that
        # is a view only binds under the name it was created with.
        alias = Path(source_path).stem
        con.execute(
            f"ATTACH '{source_path.replace(chr(39), chr(39) * 2)}' AS \"{alias}\" (READ_ONLY);"
        )
        tables = mart_tables(con, database=alias)
        if not tables:
            raise RuntimeError(
                f"no tables found in a *_{MART_SCHEMA_SUFFIX} schema of {source_path!r}. "
                "Publishing an empty serving database would replace a good copy with "
                "one that answers every question with zero rows, so this fails instead."
            )

        # `publishing`, not `serving`: the source is now attached under its own file
        # name, and a warehouse that happened to be called serving.duckdb would
        # collide with the copy being built from it.
        con.execute(f"ATTACH '{str(staged).replace(chr(39), chr(39) * 2)}' AS publishing;")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS publishing.{MART_SCHEMA_SUFFIX};")

        counts = {}
        for schema, table in tables:
            # Both identifiers come from DuckDB's own catalogue, not from input, and
            # are quoted anyway so an unusual model name cannot change the statement.
            con.execute(
                f'CREATE TABLE publishing."{MART_SCHEMA_SUFFIX}"."{table}" AS '  # noqa: S608
                f'SELECT * FROM "{alias}"."{schema}"."{table}"'
            )
            counts[table] = con.execute(
                f'SELECT count(*) FROM publishing."{MART_SCHEMA_SUFFIX}"."{table}"'  # noqa: S608
            ).fetchone()[0]
        con.execute("DETACH publishing;")
    finally:
        con.close()

    # Every mart existing says nothing about whether any of them holds a row: on 25 Oct
    # 2026 twenty-eight empty marts passed the check above and readers got zero rows for
    # a day. A mart that held rows does not go empty in a full rebuild of an append-only
    # lake, so that is a failed load upstream, and the good copy stays in place.
    rows = sum(counts.values())
    names = list(counts)
    emptied = sorted(t for t, n in counts.items() if n == 0 and previous.get(t, 0) > 0)
    if not rows or emptied:
        staged.unlink(missing_ok=True)
        raise RuntimeError(
            f"refusing to publish {source_path!r}: "
            + (f"these marts held rows and are now empty: {', '.join(emptied)}. " if emptied
               else "every mart is empty. ")
            + "The previous copy is still being served."
        )

    # Bind the semantic layer against the staged copy *before* swapping it in. The
    # tools name columns in marts they do not own, so a rename in dbt would otherwise
    # produce a serving database that publishes cleanly and fails only when somebody
    # asks the question that touches the renamed column. Failing the deploy instead
    # leaves the previous good copy serving.
    check = duckdb.connect(str(staged), read_only=True)
    try:
        check.execute("SET TimeZone='UTC';")
        problems = validate(check)
    finally:
        check.close()
    if problems:
        staged.unlink(missing_ok=True)
        raise RuntimeError(
            "the serving copy does not satisfy the semantic layer, so it was not "
            "published and the previous copy is still being served:\n  "
            + "\n  ".join(problems)
        )

    staged.replace(target)  # atomic on POSIX
    Path(str(staged) + ".wal").unlink(missing_ok=True)

    return Published(
        tables=names, rows=rows, bytes_written=target.stat().st_size, path=str(target)
    )


def _row_counts(path: Path) -> dict[str, int]:
    """Rows per mart in the copy being replaced. Empty if there is none, or it is unreadable.

    An unreadable copy is no reason to keep serving it, so it never blocks a publish.
    """
    if not path.exists():
        return {}
    con = duckdb.connect(":memory:")
    try:
        escaped = str(path).replace("'", "''")
        con.execute(f"ATTACH '{escaped}' AS previous (READ_ONLY);")
        tables = con.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = 'previous' AND schema_name = ?",
            [MART_SCHEMA_SUFFIX],
        ).fetchall()
        return {
            table: con.execute(
                f'SELECT count(*) FROM previous."{MART_SCHEMA_SUFFIX}"."{table}"'  # noqa: S608
            ).fetchone()[0]
            for (table,) in tables
        }
    except duckdb.Error:
        return {}
    finally:
        con.close()


def connect(serving_path: str = SERVING_PATH) -> duckdb.DuckDBPyConnection:
    """Open the serving copy read-only, with the same UTC pin the build uses.

    Opened by ATTACH onto an in-memory root rather than `duckdb.connect(path)`, and
    that is not a stylistic preference. DuckDB caches database instances by path
    *within a process*: once one handle is open, a later `connect()` to the same path
    joins the cached instance rather than reading the file again. After the nightly
    swap replaces the file, that means a "fresh" connection can still return last
    night's numbers for as long as any other handle is open -- measured, not feared:

        holder open, file replaced, fresh connect()  -> 200 rows (stale)
        holder open, file replaced, fresh ATTACH     ->  89 rows (correct)

    Nothing raises in the stale case, which is what makes it worth designing out.
    ATTACH opens the file directly, so reopening always reads what is on disk now.

    read_only is defence in depth rather than the main protection -- the tools only
    ever build SELECTs -- but it means a bug that managed to emit a write gets an
    error from the engine instead of changing what the next question returns.
    """
    path = Path(serving_path)
    if not path.exists():
        raise FileNotFoundError(
            f"no serving database at {serving_path!r}. It is published by the "
            "warehouse_serving asset after dbt builds; run that first."
        )
    con = duckdb.connect(":memory:")
    # The path comes from configuration, never from a caller, and is escaped anyway.
    escaped = str(path).replace("'", "''")
    con.execute(f"ATTACH '{escaped}' AS {SERVING_ALIAS} (READ_ONLY);")
    con.execute("SET TimeZone='UTC';")
    # The `datasets.*` SQL vocabulary, built here rather than on every cursor: it is
    # catalogue metadata, created once per connection, not per query. It has to
    # happen *before* `USE serving` below -- `serving` is attached read-only, so once
    # it is the current catalogue, `CREATE SCHEMA` has nowhere writable to land. The
    # views are defined in this connection's own in-memory catalogue instead, against
    # `serving.marts.*`; `search_path` (set on every cursor -- see
    # `homelab_datalake.mcp.warehouse.Warehouse.cursor`) is what then lets both
    # `marts.*` and `datasets.*` resolve unqualified from the same query.
    #
    # A dataset whose mart is not in this file yet -- a fresh checkout ahead of its
    # first dbt build -- does not stop the rest from getting a view; see
    # `create_dataset_views`. Errors are not raised here because a caller that cannot
    # get the SQL vocabulary must still be able to query through the tools that do
    # not need it, rather than losing the whole connection.
    problems = create_dataset_views(con, source_catalog=SERVING_ALIAS)
    if problems:
        logger.warning(
            "%d dataset view(s) could not be created against %s: %s",
            len(problems), serving_path, problems,
        )
    con.execute(f"USE {SERVING_ALIAS};")
    return con
