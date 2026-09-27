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

**A deadline.** DuckDB has no statement timeout, so the deadline is a watchdog thread
that interrupts the connection -- the same mechanism the MCP layer uses, for the same
reason. Together with DuckDB's memory limit it is the only bound on a result set, and
that is deliberate: the row cap that used to sit here silently returned a quarter of an
answer as though it were the whole one. A caller may still ask for a cap per call, and
one can be set for the deployment with DATALAKE_API_MAX_ROWS; neither is on by default.

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
from homelab_datalake.mcp import semantic
from homelab_datalake.resources import LakeStorage

logger = logging.getLogger(__name__)

# No row cap by default. There was one -- 1000 -- and it was the wrong control: it
# truncated an answer to a quarter of itself and the caller could not tell, because the
# `truncated` flag was returned and then dropped by the sandbox helper. A cap that is
# invisible does not bound a query, it corrupts one. The deadline below and DuckDB's
# memory limit are the real bounds, and both fail loudly.
#
# Set DATALAKE_API_MAX_ROWS to a positive integer to put a cap back; 0 means unlimited.
MAX_ROWS = max(0, int(os.environ.get("DATALAKE_API_MAX_ROWS", "0") or 0))

# Now that the row cap is gone, this is the only bound on how much a statement can
# return, so it is worth being able to raise it for a genuinely large export without
# a rebuild.
TIMEOUT_SECONDS = float(os.environ.get("DATALAKE_API_TIMEOUT_SECONDS", "30") or 30)
MAX_STATEMENTS = 1

# How much memory one session may use before DuckDB starts refusing rather than
# taking the host down with it. The deadline is the primary bound; this is what
# stops a query that is *fast* and enormous -- a cross join that fills RAM in five
# seconds is not caught by a thirty-second timer.
MEMORY_LIMIT = os.environ.get("DATALAKE_API_MEMORY_LIMIT", "2GB")

# The schemas the lake is mounted under, in the order an unqualified name resolves.
# Marts first: somebody typing a bare table name is asking about answers far more
# often than about landed records. `datasets` last: it is a much smaller vocabulary
# (fourteen names against several dozen mart tables) built *from* marts, so a bare
# name is far more likely to mean the mart than the semantic-layer view of it -- and
# the two vocabularies do not collide on a name in practice (`llm_spend` is not a
# mart table, `fct_llm_call` is not a dataset name), so the ordering mostly only
# matters for defining a tie-break rule at all.
SEARCH_PATH = (layout.LAYER_MARTS, layout.LAYER_CURATED, layout.LAYER_RAW, semantic.DATASETS_SCHEMA)

_READ_ONLY_TYPES = frozenset({duckdb.StatementType.SELECT})
_EXPLAIN = duckdb.StatementType.EXPLAIN


# --------------------------------------------------------------------------------
# comparing a column against a value it never holds
# --------------------------------------------------------------------------------
#
# A filter that matches nothing does not fail. It returns zero, and zero is a number
# that reads like an answer.
#
# On 11 Sep 2026 a weekly camera report reached an inbox saying nobody had entered or
# left all week. The SQL said `direction = 'in'`; the column holds 'entrée' and
# 'sortie'. Every SUM came back 0, the row count beside them was real, and nothing
# anywhere had to fail for that to happen. The statement was valid SQL over an
# existing column with a well-typed literal -- there is no layer below this one that
# could have objected.
#
# So this layer objects. The parser DuckDB already runs to classify the statement can
# also say which columns were compared against which literals; where the column is one
# the semantic layer describes and small enough to enumerate, a literal that is not
# among its values is refused with the values that do exist.
#
# It is deliberately narrow. Only string literals, only columns the semantic layer
# names, and only when the whole set of values is small enough to be an enumeration
# rather than a set of identifiers -- above that a filter is a lookup, and a lookup
# for something absent is an ordinary empty answer rather than a mistake.

# Above this many distinct values a column is an identifier, not an enumeration.
MAX_ENUM_VALUES = 50


def enum_columns() -> dict[str, list[str]]:
    """Column name -> the tables that describe it, from the semantic layer.

    Imported here rather than at module load: the semantic layer is a description of
    the marts, nothing else in this module needs it, and keeping the import local
    means the API keeps starting even if that description fails to import.

    Keyed by bare column name because that is all a statement gives us -- `e.direction`
    and `direction` are the same column, and the alias cannot be resolved without
    binding the query. A name used by more than one dataset collects both tables, and
    a value is only refused when it is absent from all of them.
    """
    try:
        from homelab_datalake.mcp import semantic
    except Exception:  # noqa: BLE001 - a guard that cannot load must not break querying
        logger.warning("the semantic layer could not be imported; enum guard disabled")
        return {}

    columns: dict[str, list[str]] = {}
    for dataset in semantic.DATASETS.values():
        for dimension in dataset.dimensions:
            # Only plain column references. A dimension whose sql is an expression
            # describes something computed, and the literal a caller compares it
            # against is not the column's own value.
            if not dimension.sql.isidentifier():
                continue
            columns.setdefault(dimension.sql, [])
            if dataset.table not in columns[dimension.sql]:
                columns[dimension.sql].append(dataset.table)
    return columns


def literal_comparisons(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[str, str]]:
    """Every (column, string literal) this statement compares, anywhere in it.

    Anywhere matters. The report that prompted this compared direction inside a
    `CASE WHEN` in the select list, not in the WHERE clause, so a guard that only read
    the filter would have watched the one place the mistake was not.

    Returns an empty list rather than raising if the statement cannot be serialised:
    this is a guard, and a guard that turns an unusual statement into an error is
    worse than the silence it was added to prevent.
    """
    import json

    try:
        serialised = con.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0]
        tree = json.loads(serialised)
    except (duckdb.Error, ValueError, TypeError):
        return []

    def column_of(node) -> str | None:
        if not isinstance(node, dict) or node.get("class") != "COLUMN_REF":
            return None
        names = node.get("column_names") or []
        # The last part: `e.direction` and `marts.t.direction` are both `direction`.
        return names[-1] if names else None

    def literal_of(node) -> str | None:
        if not isinstance(node, dict) or node.get("class") != "CONSTANT":
            return None
        value = node.get("value") or {}
        if value.get("is_null") or (value.get("type") or {}).get("id") != "VARCHAR":
            return None
        found = value.get("value")
        return found if isinstance(found, str) else None

    out: list[tuple[str, str]] = []

    def walk(node) -> None:
        if isinstance(node, dict):
            kind = node.get("type")
            if kind in ("COMPARE_EQUAL", "COMPARE_NOTEQUAL"):
                column, literal = column_of(node.get("left")), literal_of(node.get("right"))
                if column is None:  # the literal may be written on either side
                    column, literal = column_of(node.get("right")), literal_of(node.get("left"))
                if column and literal is not None:
                    out.append((column, literal))
            elif kind in ("COMPARE_IN", "COMPARE_NOT_IN"):
                children = node.get("children") or []
                column = column_of(children[0]) if children else None
                if column:
                    for child in children[1:]:
                        literal = literal_of(child)
                        if literal is not None:
                            out.append((column, literal))
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(tree)
    return out


def enum_refusal(comparisons, values_for) -> str | None:
    """The message refusing the first literal no column of that name ever holds.

    `values_for` returns the known values of a column, or None when there are too many
    to enumerate or they could not be read. None always means "do not judge this one":
    a guard that refuses because it could not check is a guard that blocks real work.
    """
    columns = enum_columns()
    judged: set[tuple[str, str]] = set()
    for column, literal in comparisons:
        if column not in columns or (column, literal) in judged:
            continue
        judged.add((column, literal))
        known = values_for(column)
        if known is None or literal in known:
            continue
        # Strings only. `state_changed = 'true'` against a boolean column is coerced
        # by DuckDB and is a perfectly ordinary way to write it; refusing it would be
        # this guard inventing a mistake. The one it exists to catch is a misspelt
        # word, and the column that holds words is the one whose values are strings.
        if not all(isinstance(value, str) for value in known if value is not None):
            continue
        shown = sorted(str(value) for value in known if value is not None)
        return (
            f"{column} never holds {literal!r}. It holds {shown}"
            f"{' (and null)' if any(value is None for value in known) else ''}. "
            "Comparing it against a value it does not hold matches no rows and "
            "returns zero, which cannot be told apart from a real answer -- so the "
            "statement is refused rather than answered. Use one of the values above, "
            "or ask describe_dataset for them."
        )
    return None


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

    def as_dict(self, records: bool = False) -> dict:
        """The result as JSON.

        `records` turns each row into an object keyed by column name. The default
        stays the pair of arrays, which is smaller and keeps column order, but a
        chart tool reading the response over HTTP -- Grafana's Infinity datasource,
        say -- can only map fields it can name, and cannot read a value out of a
        positional array without being told the position of every column.
        """
        return {
            "columns": self.columns,
            "types": self.types,
            "rows": [dict(zip(self.columns, row, strict=True)) for row in self.rows]
            if records else self.rows,
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

        EXPLAIN DELETE FROM t WHERE a=1            2 rows -> 2 rows   planned only
        EXPLAIN ANALYZE DELETE FROM t WHERE a=1    2 rows -> 1 row    executed
        EXPLAIN (ANALYZE) DELETE FROM t WHERE a=1  2 rows -> 1 row    executed

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


def view_create_statement(lake: LakeStorage, dataset: layout.Dataset) -> str:
    """The `CREATE OR REPLACE VIEW` statement `LakeSession._build()` runs for one
    dataset. Factored out so the choice it makes -- `union_by_name` or not -- can
    be tested directly against a handful of files, without paying for a full
    session over all of `layout.ALL_DATASETS`.

    `union_by_name` reads every matching file's Parquet footer before the
    statement returns, to compute the union schema -- measured at 18.0s across
    1,805 files for raw.jellyfin_playback alone on 2026-09-16, the single most
    expensive dataset in that incident. Skipped when `dataset.single_schema` is
    set (see that field's docstring for which datasets qualify and why): DuckDB
    then takes the schema from one file and binds without touching the rest,
    which is what a fixed writer schema makes safe. Kept everywhere else,
    because that is what lets a dataset with a genuinely evolving schema --
    dbt output, the ai_platform registry -- bind at all rather than raising the
    moment two files disagree on a column.
    """
    pattern = lake.duckdb_scan_target(dataset.prefix).replace("'", "''")
    read = (
        f"read_parquet('{pattern}')"
        if dataset.single_schema
        else f"read_parquet('{pattern}', union_by_name=true)"
    )
    return (
        f'CREATE OR REPLACE VIEW "{dataset.layer}"."{dataset.table}" AS '  # noqa: S608
        f"SELECT * FROM {read}"
    )


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
        # What each guarded column actually holds, filled in as columns are asked
        # about. The session is the right place for it: it is discarded and rebuilt
        # when the lake is republished, which is the only moment the answer can
        # change, so the cache needs no invalidation of its own.
        self._enum_values: dict[str, list | None] = {}
        self._enum_lock = threading.Lock()
        # {dataset_name: reason} for any `datasets.*` view that did not bind -- see
        # `_build()`. Exposed the same way `missing` is: named rather than silently
        # absent, so a caller that goes looking (or a future /health check) can see
        # why a name it expected is not queryable.
        self.dataset_view_problems: dict[str, str] = {}
        self._con = self._build()

    def _build(self) -> duckdb.DuckDBPyConnection:
        # TimeZone and memory_limit go in the connect configuration rather than a
        # SET, and that is load-bearing: a cursor does not inherit its parent's
        # session settings, and once the configuration is locked it cannot set them
        # for itself. Measured -- a cursor off a UTC-pinned connection reported
        # Europe/Paris, which is how a 23:30 row lands on the wrong day.
        con = duckdb.connect(":memory:", config={"TimeZone": "UTC", "memory_limit": MEMORY_LIMIT})
        # GLOBAL, because queries run on cursors and a cursor inherits none of its
        # parent's *local* settings -- which is what every s3_* setting is by
        # default. Without this the views bind on the connection and every query
        # through a cursor reads the bucket anonymously. See LakeStorage.
        self.lake.configure_duckdb(con, scope="GLOBAL")
        for layer in layout.LAYERS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {layer};")

        for dataset in layout.ALL_DATASETS:
            try:
                con.execute(view_create_statement(self.lake, dataset))
                self.mounted.append(dataset.qualified_name)
            except duckdb.Error as error:
                # A dataset with nothing landed yet is the ordinary case here, not a
                # failure. Mounting a view that cannot bind would make every query
                # against any table fail, so it is left out and named instead.
                self.missing[dataset.qualified_name] = str(error).splitlines()[0]

        # The semantic layer's own SQL vocabulary, over the marts just mounted above.
        # Same catalogue as `marts` itself here -- this is the one caller of
        # `create_dataset_views` that does not need `source_catalog`, because
        # `LakeSession` never attaches the marts from anywhere else; they are views
        # in this same in-memory connection. Must run before the guard settings
        # below: `lock_configuration` refuses `CREATE SCHEMA`/`CREATE VIEW` too, not
        # only writes to the lake.
        self.dataset_view_problems = semantic.create_dataset_views(con)

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

    def enum_values(self, column: str) -> list | None:
        """What `column` actually holds, or None when it is not worth judging.

        None is not "no values". It means do not judge this one: too many distinct
        values to be an enumeration, no dataset describing it, or a read that failed.
        In each of those a refusal would be the guard blocking real work over its own
        blind spot, which is worse than the silence it was written to prevent.

        Read lazily, one column at a time, and only when a statement actually compares
        it against a literal. Reading every dimension of every dataset up front would
        put a scan of the lake in front of the first query, to check something most
        statements never mention.
        """
        with self._enum_lock:
            if column in self._enum_values:
                return self._enum_values[column]

        values: list = []
        judged = False
        for table in enum_columns().get(column) or []:
            cursor = self.cursor()
            try:
                # Neither name is caller input: both come from the semantic layer, and
                # the column is checked to be a bare identifier where that mapping is
                # built. A caller's literal never reaches this statement.
                rows = cursor.execute(
                    f'SELECT DISTINCT "{column}" FROM {table} '  # noqa: S608
                    f"LIMIT {MAX_ENUM_VALUES + 1}"
                ).fetchall()
            except duckdb.Error:
                # A view that would not bind -- nothing landed in it yet -- says
                # nothing about the column, so it neither judges nor disqualifies it.
                continue
            finally:
                cursor.close()
            if len(rows) > MAX_ENUM_VALUES:
                judged = False  # an identifier, not an enumeration
                break
            judged = True
            for (value,) in rows:
                if value not in values:
                    values.append(value)

        known = values if judged else None
        with self._enum_lock:
            self._enum_values[column] = known
        return known

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

        # Last, because it is the only refusal here that is about what the statement
        # would *mean* rather than what it is allowed to do. A write is refused as a
        # write above; this catches the statement that is permitted, valid, and wrong
        # -- it would run, return zero, and be believed.
        refusal = enum_refusal(
            literal_comparisons(cursor, statement.query), session.enum_values
        )
        if refusal:
            raise QueryRefused(refusal, statement.type.name)

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
        if max_rows <= 0:
            # Unlimited. The deadline is still running, so a statement that streams
            # forever is interrupted rather than allowed to fill memory.
            fetched = cursor.fetchall()
            truncated = False
        else:
            fetched = cursor.fetchmany(max_rows + 1)
            truncated = len(fetched) > max_rows
            fetched = fetched[:max_rows]
        rows = [[_jsonable(value) for value in row] for row in fetched]
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


# How often the background warmer checks the publish stamp. Justified by the
# actual incident, not a round number picked for looks: the nightly publish
# finishes by about 04:20 UTC and the first automated report queries at 05:00 --
# a 40-minute cushion -- while the 2026-09-16 incident was an *ad hoc* republish
# at 17:56:46 UTC that a query hit only six minutes later. A 60s poll notices
# either kind of republish with minutes to spare: in the ad hoc case, the
# rebuild would already have been running for close to five of those six
# minutes by the time the real query arrived. The cost of polling this often is
# one os.stat() a minute -- the same call CatalogueCache already makes on every
# request -- so there is nothing to trade off against a shorter interval except
# a log line nobody reads; 60s was not narrowed further because there is no
# measured benefit to being more aggressive than the margin already is.
DEFAULT_WARM_INTERVAL_SECONDS = float(
    os.environ.get("DATALAKE_API_WARM_INTERVAL_SECONDS", "60") or 60
)


class _Handle:
    """One LakeSession plus how many callers currently hold a reference to it.

    Needed because a cached session outlives any single request: between one
    republish and the next, `get()` hands the *same* LakeSession to every
    concurrent caller, so closing it the instant a replacement is ready would
    close it out from under a query that checked it out a moment earlier.
    `refs` counts `get()` calls not yet matched by a `release()`; `retiring`
    marks a handle that has already been superseded and is only waiting for
    `refs` to reach zero before `close()` runs.
    """

    __slots__ = ("refs", "retiring", "session")

    def __init__(self, session: LakeSession) -> None:
        self.session = session
        self.refs = 0
        self.retiring = False


class SessionCache:
    """The read-only session, kept open, rebuilt in the background when the lake
    changes underneath it.

    The stamp is the same one the catalogue watches: the nightly job renames a new
    serving copy into place as its last step, so its identity changing is the one
    moment the views over the lake can be describing something out of date. Without
    this, a session opened on Monday would keep answering from the schema it bound on
    Monday until the container was restarted -- and nothing would error, which is the
    worst way for this to be wrong.

    **A republish does not make a caller wait for the rebind.** `get()` always
    returns immediately from whatever session is already serving; when the stamp
    has changed it also starts a background thread that builds the replacement,
    and only swaps it in -- atomically, under the lock -- once that build
    finishes. The one exception is the very first call with no session at all
    (a cold start): there is nothing to answer from, so that caller pays for the
    build, on whatever thread called `get()` -- unless `start_background_warming`
    is running, in which case it almost always wins that race instead (see its
    docstring): a deploy or a container restart is also a cold start, and a
    caller here is the platform's `DatalakeClient`, whose 45s timeout is shorter
    than a cold build with no `single_schema` datasets to help it.

    **Why answering from the previous session while the replacement binds is
    correct, not merely convenient.** Two things had to be checked before
    relying on this, not assumed:

    1. *Is the previous session's own serving file (`serving.duckdb`) still
       attached underneath it?* No -- `LakeSession` never attaches it.
       `serving.duckdb` is the MCP layer's artifact (`homelab_datalake.serving`,
       read by `mcp/warehouse.py`); `LakeSession._build()` mounts views
       straight over the lake's own `raw/curated/marts` prefixes via
       `lake.duckdb_scan_target(...)`. The publish stamp this class watches is
       only a *signal* -- the same file `CatalogueCache` watches, renamed into
       place as the nightly job's last step -- borrowed because its identity
       changing is the one reliable marker that the whole job, including the
       lake publish, has finished. So there is no scenario where the old
       session has a serving copy swapped out from under it: it was never
       attached to one.
    2. *Can the previous session end up reading a file that publish deleted or
       renamed?* `assets/publish.py` and `resources.py` were read to check
       this rather than assumed. A republish writes every new partition object
       first and only removes what was not rewritten afterwards
       (`_remove_stale`), and every write is a single atomic operation -- one
       PUT on S3, a write-to-temp-then-`rename()` on disk (`LakeStorage.
       write_table`). Confirmed empirically (not just read): a DuckDB view
       created over `read_parquet(glob, union_by_name=...)` is not a frozen
       snapshot of the files that matched at CREATE VIEW time -- every query
       against it re-globs and re-reads whatever is on disk *at query time*.
       Adding a file, removing one, or adding one with an extra column, all
       change what a query against an already-created view returns on the
       next call, with no error and no stale rows. So a query against the old
       session after `_remove_stale` has already run does not see a deleted
       file (the glob no longer matches it) and cannot see a half-written one
       (writes are atomic) -- it already sees the republished data for any
       table whose files existed before this run. The one real gap is a table
       that had *zero* objects when the old session was built: `LakeSession`
       records that as `missing` rather than creating a view for it (see
       `_build()`), so if this run is the table's first-ever publish, the old
       session still reports it missing until the swap completes -- the same
       "table not found" 400 the API already returns correctly for an empty
       dataset (`test_a_missing_table_comes_back_as_the_caller_s_mistake`),
       not a hang, and it clears itself within one rebuild.

    So "serving the previous session for about two minutes" understates it: for
    curated and marts tables, republished before the stamp itself changes, most
    queries during that window already see the *new* data through the old
    session's live views -- rebuilding is not what makes them current. What the
    rebuild concretely buys is a session that has a view at all for a table
    whose first-ever objects landed in this run, and a `_stamped` value that
    matches reality again, so the next stamp check is not a wasted rebuild.

    **The old session is closed only once nothing is still using it.** `get()`
    and `release()` (called by every caller in `api/app.py`, symmetrically) hold
    a per-session reference count; a superseded session moves to `_retiring` and
    is closed the moment its count reaches zero, whether that happens during the
    swap (nobody was mid-query) or later, from `release()`.

    Writable sessions are deliberately *not* cached. One is built for the statement
    and closed after it, so nothing a write left behind -- a temporary table, a
    changed setting -- is visible to the next caller.
    """

    def __init__(self, lake: LakeStorage | None = None, stamp=None) -> None:
        from homelab_datalake.api.catalogue import publish_stamp

        self._lake = lake
        self._stamp = stamp or publish_stamp
        self._lock = threading.Lock()
        self._current: _Handle | None = None
        self._stamped: tuple[int, int] | None = None
        self._retiring: list[_Handle] = []
        self._rebuilding = False

    def get(self) -> LakeSession:
        """A session to query against. Never blocks on a rebuild once warm.

        Must never be called from the event loop: the cold-start path builds a
        session inline, which is the same expensive `LakeSession._build()` this
        module exists to keep off it. Every caller in this codebase reaches
        `get()` through `starlette.concurrency.run_in_threadpool` -- see
        `api/app.py`. Pair every call with `release()`.
        """
        with self._lock:
            if self._current is None:
                # Cold start: nothing to answer from while this builds, so this
                # caller -- and only this one -- pays for it.
                self._current = _Handle(LakeSession(lake=self._lake, writable=False))
                self._stamped = self._stamp()
            else:
                self._maybe_start_rebuild()
            self._current.refs += 1
            return self._current.session

    def release(self, session: LakeSession) -> None:
        """Match a `get()`. Closes a retired session once nobody still holds it."""
        with self._lock:
            handle = self._handle_for(session)
            if handle is None:
                return  # already closed, or not a session this cache issued
            handle.refs -= 1
            if handle.retiring and handle.refs <= 0 and handle in self._retiring:
                self._retiring.remove(handle)
                handle.session.close()

    def maybe_warm(self) -> None:
        """Notice a republish and start rebinding, without ever blocking.

        This is what `start_background_warming`'s poll loop calls on every
        tick, and it is also safe to call directly from the event loop --
        unlike `get()` -- because it never builds a session inline: a cold
        cache (no session at all yet) is left alone here. That is deliberate
        for *this* method specifically, not for warming as a whole -- a
        caller of `maybe_warm()` might be on the event loop (a future `/health`
        hook, say), and it must never pay for a ~105s cold build. The cold
        case is instead handled once, eagerly, by `start_background_warming`
        itself, in its own thread, before this method's polling ever starts --
        see there for why leaving it to `maybe_warm()` was not enough.
        """
        with self._lock:
            if self._current is not None:
                self._maybe_start_rebuild()

    def start_background_warming(
        self, interval: float = DEFAULT_WARM_INTERVAL_SECONDS
    ) -> threading.Thread:
        """Build a session immediately if the cache is cold, then poll the
        publish stamp on a timer so a later republish starts rebinding before
        any caller asks a question that needs it.

        **The eager build matters on its own, separately from the polling.**
        A cold cache is not only the process's first moment alive -- it is
        also every deploy and every container restart, and without this the
        first real caller after one of those pays for the ~105s cold build
        (measured pre-`single_schema`) inline. `maybe_warm()` deliberately
        will not do that build for a caller that might be on the event loop,
        but this method runs entirely in its own thread, so there is nothing
        wrong with it blocking *that* thread on the same cold-start path
        `get()` uses -- `self.get()` immediately followed by `self.release()`,
        which builds under `self._lock` exactly as a real caller's cold
        `get()` would, then drops the reference since nobody is actually
        querying. A real caller's `get()` arriving mid-build simply blocks on
        that same lock -- on its own worker thread, per `run_in_threadpool`,
        never the event loop -- and then receives the session this thread just
        finished, rather than racing to build a second one; measured against
        that, it can only be a shorter wait than building it itself, never a
        longer one.

        **Then it polls**, independently of traffic, so a *later* republish
        does not wait for `get()` to notice it either -- the *caller* of that
        `get()` would otherwise be whichever request happens to land first
        after the nightly build, and there is no reason that has to be a
        query rather than, say, the automated report itself. See
        `DEFAULT_WARM_INTERVAL_SECONDS` for why 60s specifically. Returns the
        thread so a caller that wants to (tests, mainly) can join or inspect
        it; production code does not need to.
        """
        def loop() -> None:
            try:
                session = self.get()
                self.release(session)
            except Exception:
                logger.exception(
                    "eager session warm-up failed; the first caller will build it"
                )
            while True:
                time.sleep(interval)
                try:
                    self.maybe_warm()
                except Exception:
                    logger.exception("background session warm check failed")

        thread = threading.Thread(
            target=loop, daemon=True, name="datalake-session-warmer"
        )
        thread.start()
        return thread

    # -- internals, only ever called with self._lock held ------------------------

    def _maybe_start_rebuild(self) -> None:
        stamp = self._stamp()
        if stamp == self._stamped or self._rebuilding:
            return
        self._rebuilding = True
        threading.Thread(
            target=self._rebuild, args=(stamp,), daemon=True,
            name="datalake-session-rebind",
        ).start()

    def _rebuild(self, stamp) -> None:
        """Build the replacement with the lock released, then swap it in under it.

        Building is the slow part -- a footer read per file for every dataset
        without `single_schema`, 104.7s measured across 168 datasets on
        2026-09-16 -- and it runs off the lock so every `get()` and `release()`
        in the meantime is served by the session already running, rather than
        queued behind a build nobody asked to wait for.
        """
        try:
            replacement = LakeSession(lake=self._lake, writable=False)
        except Exception:
            logger.exception(
                "background session rebind failed; still serving the previous session"
            )
            with self._lock:
                self._rebuilding = False
            return

        with self._lock:
            old = self._current
            self._current = _Handle(replacement)
            self._stamped = stamp
            self._rebuilding = False
            logger.info("the lake was republished; rebound the query session")
            if old is not None:
                old.retiring = True
                if old.refs > 0:
                    self._retiring.append(old)
                else:
                    old.session.close()

    def _handle_for(self, session: LakeSession) -> _Handle | None:
        if self._current is not None and self._current.session is session:
            return self._current
        return next((h for h in self._retiring if h.session is session), None)

    def writable(self) -> LakeSession:
        """A fresh session that may write. The caller closes it.

        Building this is exactly as expensive as the read-only rebuild above --
        same `LakeSession._build()` -- so a caller on the event loop must run
        this through `run_in_threadpool` too.
        """
        return LakeSession(lake=self._lake, writable=True)

    def close(self) -> None:
        """Close everything, regardless of outstanding references. For shutdown
        and test teardown, not for the request path."""
        with self._lock:
            if self._current is not None:
                self._current.session.close()
                self._current = None
            for handle in self._retiring:
                handle.session.close()
            self._retiring = []
