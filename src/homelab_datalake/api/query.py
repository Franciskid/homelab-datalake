"""What a caller may run, and under what limits.

The MCP layer next door never needs any of this: it composes the SQL itself, so a
question it cannot express is a question that cannot be asked. This module accepts
SQL a person wrote, which means every property that came free over there has to be
established here, explicitly, and tested.

Four controls, and each closes a different hole.

**The statement is classified by DuckDB's own parser.** `extract_statements()` is the
same parser that will run it, so a `DELETE` hidden behind a block comment, unusual
whitespace or a dollar-quoted string is a DELETE here too. A regular expression
looking for the word "delete" is a different program from the one that executes, and
the gap between the two is where the bypass lives.

`EXPLAIN` is recursive rather than waved through, and that is not pedantry --
`EXPLAIN ANALYZE DELETE FROM t` classifies as EXPLAIN and *executes the delete*.
Measured, not assumed. So an EXPLAIN is permitted exactly when what it explains is
permitted, decided by handing the inner statement back to the same parser.

**The filesystem is disabled and the configuration is locked.** A `SELECT` is not
harmless if it can be `SELECT * FROM read_csv('/etc/shadow')`. `disabled_filesystems`
closes that, and `lock_configuration` is what stops the caller from turning it back
on -- one without the other is a suggestion. They are set last, after the views are
built, because the lock also prevents this module from finishing its own setup.

**One statement per call.** Not a limit on ambition: it is what makes the audit log a
record of what ran rather than of what was submitted, and what stops the second half
of a payload from being something the first half made look harmless.

**A deadline and a row cap.** DuckDB has no statement timeout, so the deadline is a
watchdog thread that interrupts the connection -- the same mechanism the MCP layer
uses, for the same reason. The row cap is applied by fetching, not by rewriting the
caller's SQL: DuckDB streams results, so asking for [count withheld] rows of a 20-million-row
scan returns in milliseconds without materialising the rest.

Writes are a separate decision, taken twice: the deployment must permit them at all
(`DATALAKE_API_ALLOW_WRITES`), and the caller must ask for them per statement. The
first exists so the switch stays bolted shut until the lake has a tested restore --
a raw partition is not rebuildable, and a backup nobody has restored is not a backup.
"""

from __future__ import annotations

import datetime as dt
import decimal
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field

import duckdb

from homelab_datalake import layout
from homelab_datalake.resources import LakeStorage

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
TIMEOUT_SECONDS = 30.0
MAX_STATEMENTS = 1

# How much memory one session may use before DuckDB starts refusing rather than
# taking the host down with it. The deadline is the primary bound; this is what
# stops a query that is *fast* and enormous -- a cross join that fills RAM in five
# seconds is not caught by a thirty-second timer.
MEMORY_LIMIT = os.environ.get("DATALAKE_API_MEMORY_LIMIT", "2GB")

# The schemas the lake is mounted under, in the order an unqualified name resolves.
# Marts first: somebody typing a bare table name is asking about answers far more
# often than about landed records.
SEARCH_PATH = (layout.LAYER_MARTS, layout.LAYER_CURATED, layout.LAYER_RAW)

_READ_ONLY_TYPES = frozenset({duckdb.StatementType.SELECT})
_EXPLAIN = duckdb.StatementType.EXPLAIN


def writes_are_released() -> bool:
    """Whether this deployment permits the switch to be turned off at all.

    Default no. Phase 4 of the workbench plan -- versioning, a daily copy of raw, and
    a restore that somebody has actually run -- has to be in place before a person
    can be handed a switch that deletes a partition the source no longer has.
    """
    return os.environ.get("DATALAKE_API_ALLOW_WRITES", "").strip().lower() in ("1", "yes", "true")


class QueryRefused(ValueError):
    """The statement was not run. The message says which rule refused it.

    Carries the statement type when the parser got far enough to know it, because a
    refusal is the half of the audit log worth reading -- "somebody tried a DELETE"
    is the record, and "somebody sent something that was refused" is not.
    """

    def __init__(self, message: str, statement_type: str = "") -> None:
        super().__init__(message)
        self.statement_type = statement_type


@dataclass
class Result:
    columns: list[str]
    types: list[str]
    rows: list[list]
    row_count: int
    truncated: bool
    elapsed_ms: float
    statement_type: str
    read_only: bool
    sql: str
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "columns": self.columns,
            "types": self.types,
            "rows": self.rows,
            "row_count": self.row_count,
            "truncated": self.truncated,
            "elapsed_ms": round(self.elapsed_ms, 1),
            "statement_type": self.statement_type,
            "read_only": self.read_only,
            "sql": self.sql,
            "notes": self.notes,
        }


# --------------------------------------------------------------------------------
# classification
# --------------------------------------------------------------------------------


def single_statement(con: duckdb.DuckDBPyConnection, sql: str) -> duckdb.Statement:
    """Parse, and insist on exactly one statement.

    Parsing here rather than splitting on semicolons: a semicolon inside a string
    literal or a dollar-quoted body is not a separator, and a splitter that thinks it
    is will either refuse valid SQL or -- worse -- hand the engine something it did
    not check.
    """
    if not sql.strip():
        raise QueryRefused("no statement was supplied.")
    try:
        statements = con.extract_statements(sql)
    except duckdb.Error as error:
        raise QueryRefused(f"the statement could not be parsed: {error}") from None
    if not statements:
        raise QueryRefused("no statement was supplied.")
    if len(statements) > MAX_STATEMENTS:
        raise QueryRefused(
            f"{len(statements)} statements were supplied and the limit is "
            f"{MAX_STATEMENTS}. Run them one at a time: the audit log records what "
            "ran, and a batch makes that record ambiguous."
        )
    return statements[0]


# How far into an EXPLAIN to look for the statement it explains. Long enough for
# `EXPLAIN (FORMAT json, ANALYZE)`, short enough that this never wanders into the
# body of the statement and finds some parseable fragment of it.
_EXPLAIN_PREFIX_TOKENS = 8


def _inner_of_explain(con: duckdb.DuckDBPyConnection, statement: duckdb.Statement):
    """The statement an EXPLAIN explains, re-parsed rather than pattern-matched.

    This matters because EXPLAIN is not always inert. Measured on DuckDB 1.5:

        EXPLAIN DELETE FROM t WHERE a=1            [count withheld] rows -> [count withheld] rows   planned only
        EXPLAIN ANALYZE DELETE FROM t WHERE a=1    [count withheld] rows -> [count withheld] row    executed
        EXPLAIN (ANALYZE) DELETE FROM t WHERE a=1  [count withheld] rows -> [count withheld] row    executed

    All three classify as StatementType.EXPLAIN, so a rule that permits EXPLAIN
    permits a delete. Rather than hunt for the word ANALYZE -- which the parenthesised
    form and a comment between the keywords both defeat -- this finds the inner
    statement and asks the parser what it is.

    `duckdb.tokenize` gives byte offsets for the real tokens, comments and whitespace
    already skipped. The suffix beginning at each of the first few tokens is offered
    to the parser, and the first one that parses is the inner statement: the option
    list of an EXPLAIN is not itself a statement, so nothing before it can parse.

    Returns None when nothing does, which the caller refuses. A shape the parser
    accepts as an EXPLAIN but whose body it will not parse alone is not one to reason
    about optimistically.
    """
    text = statement.query
    tokens = duckdb.tokenize(text)
    for offset, _kind in tokens[1:_EXPLAIN_PREFIX_TOKENS + 1]:
        try:
            inner = con.extract_statements(text[offset:])
        except duckdb.Error:
            continue
        if inner:
            return inner[0]
    return None


def is_read_only(con: duckdb.DuckDBPyConnection, statement: duckdb.Statement) -> bool:
    """Whether this statement may run with the switch on.

    An allow-list of one type, plus EXPLAIN of something that is itself allowed.
    Everything else -- INSERT, UPDATE, DELETE, MERGE, COPY, ATTACH, CREATE, DROP,
    CALL, PRAGMA-that-sets, LOAD -- is refused by not being on it. That direction
    matters: a deny-list has to be revised every time DuckDB grows a statement type,
    and the revision that gets forgotten is the hole.
    """
    if statement.type in _READ_ONLY_TYPES:
        return True
    if statement.type == _EXPLAIN:
        inner = _inner_of_explain(con, statement)
        return inner is not None and is_read_only(con, inner)
    return False


# --------------------------------------------------------------------------------
# the session
# --------------------------------------------------------------------------------


def guard_settings(lake: LakeStorage) -> dict[str, str]:
    """The settings that make a session read-only, for this lake.

    `disabled_filesystems` names the local filesystem in production, where the lake
    is on S3 and nothing legitimate reads a path on the server. It cannot when the
    lake *is* a local directory -- development and CI -- because the same setting
    that stops `read_csv('/etc/passwd')` also stops reading the data. That is a real
    difference in posture between the two, stated here rather than hidden: the
    deployment that holds data is the one that is locked down.
    """
    settings = {"lock_configuration": "true"}
    if lake.uses_s3:
        settings = {"disabled_filesystems": "LocalFileSystem", **settings}
    return settings


class LakeSession:
    """A DuckDB connection with the whole lake mounted as raw, curated and marts.

    Long-lived on purpose. Every view is bound when it is created, which reads the
    Parquet footers -- the same read the catalogue does -- so building this per
    request would put a second of MinIO traffic in front of every keystroke. It is
    rebuilt when the lake is republished, which is the only moment its answer can
    change.
    """

    def __init__(self, lake: LakeStorage | None = None, writable: bool = False) -> None:
        self.lake = lake or LakeStorage.from_env()
        self.writable = writable
        self.mounted: list[str] = []
        self.missing: dict[str, str] = {}
        self._con = self._build()

    def _build(self) -> duckdb.DuckDBPyConnection:
        # TimeZone and memory_limit go in the connect configuration rather than a
        # SET, and that is load-bearing: a cursor does not inherit its parent's
        # session settings, and once the configuration is locked it cannot set them
        # for itself. Measured -- a cursor off a UTC-pinned connection reported
        # Europe/Paris, which is how a 23:[count withheld] row lands on the wrong day.
        con = duckdb.connect(":memory:", config={"TimeZone": "UTC", "memory_limit": MEMORY_LIMIT})
        # GLOBAL, because queries run on cursors and a cursor inherits none of its
        # parent's *local* settings -- which is what every s3_* setting is by
        # default. Without this the views bind on the connection and every query
        # through a cursor reads the bucket anonymously. See LakeStorage.
        self.lake.configure_duckdb(con, scope="GLOBAL")
        for layer in layout.LAYERS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {layer};")

        for dataset in layout.ALL_DATASETS:
            pattern = self.lake.duckdb_scan_target(dataset.prefix).replace("'", "''")
            try:
                con.execute(
                    f'CREATE OR REPLACE VIEW "{dataset.layer}"."{dataset.table}" AS '  # noqa: S608
                    f"SELECT * FROM read_parquet('{pattern}', union_by_name=true)"
                )
                self.mounted.append(dataset.qualified_name)
            except duckdb.Error as error:
                # A dataset with nothing landed yet is the ordinary case here, not a
                # failure. Mounting a view that cannot bind would make every query
                # against any table fail, so it is left out and named instead.
                self.missing[dataset.qualified_name] = str(error).splitlines()[0]

        if not self.writable:
            for name, value in guard_settings(self.lake).items():
                # lock_configuration comes last by construction: dict order. Setting
                # it earlier would refuse the settings after it.
                con.execute(f"SET {name}='{value}';")
        return con

    def cursor(self) -> duckdb.DuckDBPyConnection:
        """A handle per query. DuckDB connections are not safe to share across threads.

        The search path is set per cursor because a cursor inherits neither the
        catalogue nor the search path of the connection it came from. It is not a
        configuration option, so the lock does not block it.
        """
        cursor = self._con.cursor()
        cursor.execute(f"SET search_path='{','.join(SEARCH_PATH)}';")
        return cursor

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> LakeSession:
        return self

    def __exit__(self, *exception) -> None:
        self.close()


# --------------------------------------------------------------------------------
# running one
# --------------------------------------------------------------------------------


def run(
    session: LakeSession,
    sql: str,
    read_only: bool = True,
    max_rows: int = MAX_ROWS,
    timeout: float = TIMEOUT_SECONDS,
) -> Result:
    """Classify, refuse or run, and cap what comes back."""
    cursor = session.cursor()
    try:
        statement = single_statement(cursor, sql)
        allowed = is_read_only(cursor, statement)

        if read_only and not allowed:
            raise QueryRefused(
                f"the read-only switch is on, and this is a {statement.type.name} "
                "statement. Only SELECT, and EXPLAIN of a SELECT, run with the switch "
                "on. Turn the switch off to write to the lake.",
                statement.type.name,
            )
        if not read_only and not writes_are_released():
            raise QueryRefused(
                "writes are not enabled on this deployment. The lake holds raw data "
                "that cannot be rebuilt -- the proxy logs rotate off the disk and the "
                "camera serves only a recent window -- so the switch stays shut until "
                "the backup and the restore are in place. Set "
                "DATALAKE_API_ALLOW_WRITES=yes when they are.",
                statement.type.name,
            )
        if not read_only and session.writable is False:
            raise QueryRefused(
                "this session was built read-only. A write needs a writable session; "
                "that is a bug in the caller rather than in the statement.",
                statement.type.name,
            )

        notes: list[str] = []
        if not read_only:
            notes.append(
                "The switch was off, so this statement could change the lake. The "
                "warehouse still holds what it held before: run analytics_daily to "
                "rebuild it before trusting anything that reads it."
            )

        began = time.perf_counter()
        columns, types, rows, truncated = _execute_bounded(cursor, statement.query, max_rows, timeout)
        elapsed = (time.perf_counter() - began) * 1000

        if truncated:
            notes.append(
                f"Truncated to {max_rows} rows. This is not the whole answer -- "
                "aggregate or filter before drawing a conclusion from it."
            )

        return Result(
            columns=columns,
            types=types,
            rows=rows,
            row_count=len(rows),
            truncated=truncated,
            elapsed_ms=elapsed,
            statement_type=statement.type.name,
            read_only=read_only,
            sql=statement.query.strip(),
            notes=notes,
        )
    finally:
        cursor.close()


def _execute_bounded(cursor, sql: str, max_rows: int, timeout: float):
    """Run the statement, interrupting it if it outlives the deadline.

    DuckDB has no statement timeout, so this is a watchdog: a timer that calls
    interrupt() on the cursor. Without it the deadline would be a comment.

    Only `max_rows + 1` rows are fetched, and DuckDB streams, so the extra row that
    detects truncation costs one row rather than the rest of the scan.
    """
    timer = threading.Timer(timeout, cursor.interrupt)
    timer.start()
    try:
        cursor.execute(sql)
        if cursor.description is None:
            # A statement that returns nothing -- some writes do. Reported as zero
            # rows rather than as an error.
            return [], [], [], False
        columns = [d[0] for d in cursor.description]
        types = [str(d[1]) for d in cursor.description]
        fetched = cursor.fetchmany(max_rows + 1)
        truncated = len(fetched) > max_rows
        rows = [[_jsonable(value) for value in row] for row in fetched[:max_rows]]
        return columns, types, rows, truncated
    except duckdb.InterruptException:
        raise QueryRefused(
            f"the statement exceeded the {timeout:g}s limit and was cancelled. "
            "Narrow the date range, or aggregate instead of selecting rows."
        ) from None
    except duckdb.Error as error:
        # The engine refused it: a table that is not there, a type that will not
        # cast, a syntax the parser accepted and the binder did not. Passed through
        # as the caller's mistake rather than a server fault, with DuckDB's own
        # message, which names the column and usually suggests the right one.
        raise QueryRefused(str(error).strip()) from None
    finally:
        timer.cancel()


def _jsonable(value):
    """Types DuckDB returns that JSON does not carry.

    DECIMAL becomes float: at homelab magnitudes a float64 carries these exactly, and
    the alternative -- a string -- makes every consumer parse it back before it can
    plot anything. Bytes become a length rather than a mangled string, because a
    BLOB in a result grid is noise whatever it is rendered as.
    """
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


class SessionCache:
    """The read-only session, kept open, rebuilt when the lake changes underneath it.

    The stamp is the same one the catalogue watches: the nightly job renames a new
    serving copy into place as its last step, so its identity changing is the one
    moment the views over the lake can be describing something out of date. Without
    this, a session opened on Monday would keep answering from the schema it bound on
    Monday until the container was restarted -- and nothing would error, which is the
    worst way for this to be wrong.

    Writable sessions are deliberately *not* cached. One is built for the statement
    and closed after it, so nothing a write left behind -- a temporary table, a
    changed setting -- is visible to the next caller.
    """

    def __init__(self, lake: LakeStorage | None = None, stamp=None) -> None:
        from homelab_datalake.api.catalogue import publish_stamp

        self._lake = lake
        self._stamp = stamp or publish_stamp
        self._lock = threading.Lock()
        self._session: LakeSession | None = None
        self._stamped: tuple[int, int] | None = None

    def get(self) -> LakeSession:
        with self._lock:
            stamp = self._stamp()
            if self._session is None or stamp != self._stamped:
                if self._session is not None:
                    logger.info("the lake was republished; rebinding the query session")
                    self._session.close()
                self._session = LakeSession(lake=self._lake, writable=False)
                self._stamped = stamp
            return self._session

    def writable(self) -> LakeSession:
        """A fresh session that may write. The caller closes it."""
        return LakeSession(lake=self._lake, writable=True)

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                self._session.close()
                self._session = None
