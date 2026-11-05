"""What an hourly run has to redo, so it stops rewriting what has not changed.

analytics_hourly used to replace every raw table and every mart each hour. That wrote
about 12 GB an hour (290 GB a day) to the server's disk, measured on 5 Nov 2026, though
the tables that make up nearly all of it land once a night: npm_access and the Jellyfin
library are 2 GB of the warehouse and a further 2 GB of marts, and the sources that do
land hourly are a few MB between them.

So each source now has a fingerprint of its objects in the lake:

    loaded   the fingerprint the raw table was last read from
    built    the fingerprint the dbt models were last built from

A source whose objects did not change is not reloaded. A source with loaded != built
has its downstream models rebuilt, and marking them built happens only after dbt
succeeds, so a failed build is retried by the next run rather than forgotten.

Three things still rebuild everything, because skipping work must never be the reason
the warehouse disagrees with the lake:

* the first run from 03:00 UTC each day, which is also when the nightly sources land,
* any change to this package or the dbt project, because the code decides the output,
* a run tagged `datalake/full_rebuild=true`, for an operator.

Losing this state is safe. It lives in the warehouse file, which is a build artefact,
and a warehouse without it gets a full rebuild.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

SCHEMA = "build_state"
FULL_REBUILD_TAG = "datalake/full_rebuild"

# Nightly sources are done by 03:45 and picked up by the 03:55 run, so the daily full
# rebuild lands on the run that would have been heavy anyway.
FULL_REBUILD_FROM_HOUR_UTC = 3

_PACKAGE_ROOT = Path(__file__).resolve().parent
_DBT_ROOT = _PACKAGE_ROOT.parents[1] / "dbt"
_DBT_INPUTS = ("models", "tests", "macros", "dbt_project.yml", "packages.yml", "package-lock.yml")


@dataclass(frozen=True)
class Pending:
    """What dbt has to do. `full` means every model, whatever `sources` holds."""

    full: bool
    sources: tuple[str, ...]


@dataclass(frozen=True)
class BuildPlan:
    """A partial dbt build, worked out from the manifest's own graph."""

    rebuilds_anything: bool
    marts_changed: bool
    exclude: tuple[str, ...]
    untouched_models: frozenset[str]


def warehouse_path() -> str:
    """The same path warehouse.DUCKDB_PATH resolves, without importing that module.

    warehouse.py imports the dbt assets, and the dbt assets need this.
    """
    return str(Path(os.environ.get("DUCKDB_PATH", "warehouse/datalake.duckdb")).resolve())


def code_version() -> str:
    """A hash of everything that decides what the warehouse should hold."""
    digest = hashlib.sha256()
    files = list(_PACKAGE_ROOT.rglob("*.py"))
    for name in _DBT_INPUTS:
        path = _DBT_ROOT / name
        files += [p for p in path.rglob("*") if p.is_file()] if path.is_dir() else [path]
    for path in sorted(p for p in files if p.is_file() and "__pycache__" not in p.parts):
        digest.update(str(path.relative_to(_PACKAGE_ROOT.parents[1])).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def fingerprint(lake_fingerprint: str, load_statement: str) -> str:
    """One source's fingerprint. The statement counts: it decides which rows are kept."""
    return hashlib.sha256(f"{lake_fingerprint}\n{load_statement}".encode()).hexdigest()


def ensure(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.sources "
        "(name VARCHAR PRIMARY KEY, loaded VARCHAR, built VARCHAR);"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.meta (key VARCHAR PRIMARY KEY, value VARCHAR);"
    )


def _meta(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    return dict(con.execute(f"SELECT key, value FROM {SCHEMA}.meta").fetchall())  # noqa: S608


def _set_meta(con: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    con.execute(f"INSERT OR REPLACE INTO {SCHEMA}.meta VALUES (?, ?)", [key, value])  # noqa: S608


def full_rebuild_due(meta: dict[str, str], code: str, now: dt.datetime, forced: bool) -> bool:
    """Whether this run must redo everything. See the module docstring for why each."""
    if forced or meta.get("code_version") != code:
        return True
    return (
        now.hour >= FULL_REBUILD_FROM_HOUR_UTC
        and meta.get("full_rebuild_day") != now.date().isoformat()
    )


def begin_load(
    con: duckdb.DuckDBPyConnection, *, code: str, now: dt.datetime, forced: bool = False
) -> bool:
    """Start a run, forgetting every fingerprint if it is a full one. Returns whether it is.

    Forgetting rather than remembering "this run is full" is what makes a crash safe: a
    run that dies halfway leaves the rest of the sources with no fingerprint, so the next
    one finishes the job without having to know a previous run failed.
    """
    ensure(con)
    if not full_rebuild_due(_meta(con), code, now, forced):
        return False
    con.execute("BEGIN")
    con.execute(f"UPDATE {SCHEMA}.sources SET loaded = NULL, built = NULL")  # noqa: S608
    _set_meta(con, "code_version", code)
    _set_meta(con, "full_build_pending", "1")
    # Only a run that was due by the clock settles the day. A deploy at noon must not
    # cancel the night's rebuild, or a day's worth of drift could go uncaught.
    if now.hour >= FULL_REBUILD_FROM_HOUR_UTC:
        _set_meta(con, "full_rebuild_day", now.date().isoformat())
    con.execute("COMMIT")
    return True


def loaded(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Each source's fingerprint, for those whose raw table is known to match it."""
    rows = con.execute(
        f"SELECT name, loaded FROM {SCHEMA}.sources WHERE loaded IS NOT NULL"  # noqa: S608
    ).fetchall()
    return dict(rows)


def record_loaded(con: duckdb.DuckDBPyConnection, name: str, value: str) -> None:
    con.execute(
        f"INSERT INTO {SCHEMA}.sources (name, loaded) VALUES (?, ?) "  # noqa: S608
        "ON CONFLICT (name) DO UPDATE SET loaded = excluded.loaded",
        [name, value],
    )


def pending(path: str | None = None) -> Pending:
    """What dbt has to build. A warehouse with no state yet needs all of it."""
    con = duckdb.connect(path or warehouse_path())
    try:
        tables = {
            name for (name,) in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = ?", [SCHEMA]
            ).fetchall()
        }
        if not {"sources", "meta"} <= tables or _meta(con).get("full_build_pending"):
            return Pending(full=True, sources=())
        rows = con.execute(
            f"SELECT name FROM {SCHEMA}.sources "  # noqa: S608
            "WHERE loaded IS DISTINCT FROM built ORDER BY name"
        ).fetchall()
        return Pending(full=False, sources=tuple(name for (name,) in rows))
    finally:
        con.close()


def record_built(path: str | None = None, *, marts_changed: bool) -> None:
    """dbt succeeded: what was loaded is now built. Call nothing here if it failed."""
    con = duckdb.connect(path or warehouse_path())
    try:
        ensure(con)
        con.execute("BEGIN")
        con.execute(f"UPDATE {SCHEMA}.sources SET built = loaded")  # noqa: S608
        con.execute(f"DELETE FROM {SCHEMA}.meta WHERE key = 'full_build_pending'")  # noqa: S608
        if marts_changed:
            _set_meta(con, "marts_generation", uuid.uuid4().hex)
        con.execute("COMMIT")
    finally:
        con.close()


def marts_generation(path: str) -> str | None:
    """Changes whenever a mart was rebuilt. None if this warehouse has never recorded one.

    Attached read-only, like every other step after dbt: they run at the same time, and
    a read-write open would take the lock from them.
    """
    con = duckdb.connect(":memory:")
    try:
        escaped = path.replace("'", "''")
        con.execute(f"ATTACH '{escaped}' AS warehouse (READ_ONLY);")
        try:
            row = con.execute(
                f"SELECT value FROM warehouse.{SCHEMA}.meta "  # noqa: S608
                "WHERE key = 'marts_generation'"
            ).fetchone()
        except duckdb.CatalogException:
            return None
        return row[0] if row else None
    finally:
        con.close()


def plan_build(manifest_path: Path, sources: tuple[str, ...]) -> BuildPlan:
    """What dbt must and must not touch when these sources changed.

    Raw tables nothing reads are in the warehouse too, so a source changing does not
    always mean there is anything for dbt to do.
    """
    manifest = json.loads(Path(manifest_path).read_text())
    roots = [
        unique_id
        for unique_id, props in manifest["sources"].items()
        if props["source_name"] == "raw" and props["name"] in sources
    ]
    downstream: set[str] = set()
    stack = list(roots)
    while stack:
        for child in manifest["child_map"].get(stack.pop(), []):
            if child not in downstream:
                downstream.add(child)
                stack.append(child)
    nodes = manifest["nodes"]
    buildable = {
        unique_id: props
        for unique_id, props in nodes.items()
        if props["resource_type"] in ("model", "test", "seed", "snapshot")
    }
    rebuilt = [unique_id for unique_id in buildable if unique_id in downstream]
    return BuildPlan(
        rebuilds_anything=bool(rebuilt),
        marts_changed=any(
            buildable[unique_id]["resource_type"] == "model"
            and buildable[unique_id]["config"]["materialized"] == "table"
            for unique_id in rebuilt
        ),
        exclude=tuple(sorted(
            props["name"] for unique_id, props in buildable.items() if unique_id not in downstream
        )),
        untouched_models=frozenset(
            props["name"]
            for unique_id, props in buildable.items()
            if props["resource_type"] == "model" and unique_id not in downstream
        ),
    )
