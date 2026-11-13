"""Every dataset in the lake, with its columns, its types and what they mean.

Two sources of truth are joined here, and neither is sufficient alone:

* **The Parquet footers** say which columns exist and what type they are. This is
  the only honest answer -- a schema declared in YAML is what somebody intended, and
  the footer is what was written. An autocomplete built from intent offers columns
  that are not there.
* **The dbt manifest** says what a column *means*. Footers carry no prose, and a
  column list without descriptions makes the reader guess whether `client_kind` is
  the browser family or the bot-versus-human split.

The join key is the table name, which is already the name the lake, DuckDB and dbt
all use for the same thing -- see `homelab_datalake.layout`.

**Why this is cached.** Only registered dataset prefixes are listed; the rest of
this bucket contains hundreds of thousands of inbox and attachment objects. Footer
results are cached by the exact object inventory (including ETags), so a publish
rereads only objects whose fingerprints changed. Bounded workers inspect those objects
in parallel, and an optional persistent cache survives API/container restarts.

**How it knows to rebuild.** The lake changes exactly once a night, when the publish
assets rewrite it -- and the last thing that job does is swap in a new serving copy.
So the cache watches that file's identity, the same trick `mcp/warehouse.py` uses to
notice the swap. That is a stat() per request instead of a callback Dagster has to
make, hold a token for, and retry: there is no refresh that can be lost, because
there is no refresh being sent. A timer expiry backs it up for the case where the
serving file is not mounted, and `POST /catalogue/refresh` forces one by hand.

**Why nothing here builds a path.** Every prefix comes from `layout.prefix()`. A
path assembled in this module could disagree with the one the publisher writes to,
and the failure mode of that disagreement is an empty catalogue entry for a dataset
that is perfectly healthy.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import duckdb

from homelab_datalake import layout, serving
from homelab_datalake.resources import LakeStorage

logger = logging.getLogger(__name__)

# The manifest written by `dbt parse` at image build time. Resolved relative to this
# file rather than to the working directory, because the API process and the Dagster
# process do not share one -- the same reason DUCKDB_PATH is made absolute.
DEFAULT_MANIFEST_PATH = str(
    Path(os.environ.get("DBT_MANIFEST_PATH")
         or Path(__file__).resolve().parents[3] / "dbt" / "target" / "manifest.json")
)

# One hour. Not the freshness policy -- the publish stamp below is what keeps the
# catalogue current. This is the backstop for a deployment where the serving file is
# not mounted, so the answer there is "up to an hour stale" rather than "stale until
# the container is restarted".
DEFAULT_TTL_SECONDS = 3600.0


def publish_stamp(path: str = serving.SERVING_PATH) -> tuple[int, int] | None:
    """A value that changes when the nightly publish finishes, or None if it cannot tell.

    `warehouse_serving` writes a temporary file and renames it into place, so both the
    inode and the modification time change at the instant the job completes -- and
    neither changes at any other time. Returning None when the file is absent is what
    makes the API runnable without the warehouse volume; the caller then falls back to
    the timer.
    """
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return (stat.st_ino, stat.st_mtime_ns)


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str

    def as_dict(self) -> dict:
        return {"name": self.name, "type": self.type, "description": self.description}


@dataclass(frozen=True)
class Table:
    """One dataset, as the workbench needs to see it."""

    table: str
    qualified_name: str
    layer: str
    domain: str
    source: str
    prefix: str
    description: str
    partition_column: str | None
    columns: tuple[Column, ...]
    rows: int
    objects: int
    bytes: int
    # The Dagster asset that writes these objects, as path segments. The workbench
    # links to it; see ASSET_PATHS for why it is not derived from the layer.
    asset_path: tuple[str, ...]
    # Set when the footers could not be read: no objects landed yet, or the read
    # failed. The entry is still returned -- a dataset that exists and is empty is a
    # fact worth showing, and dropping it would make the tree silently incomplete.
    unavailable: str | None = None

    def as_dict(self) -> dict:
        return {
            "table": self.table,
            "qualified_name": self.qualified_name,
            "layer": self.layer,
            "domain": self.domain,
            "source": self.source,
            "prefix": self.prefix,
            "description": self.description,
            "partition_column": self.partition_column,
            "columns": [c.as_dict() for c in self.columns],
            "rows": self.rows,
            "objects": self.objects,
            "bytes": self.bytes,
            "asset_path": list(self.asset_path),
            "unavailable": self.unavailable,
        }


@dataclass(frozen=True)
class Catalogue:
    tables: tuple[Table, ...]
    built_at: float
    build_ms: float
    lake: str
    # False when the manifest could not be read. Surfaced rather than swallowed: a
    # catalogue with every description missing looks like unwritten documentation,
    # and is usually a path that moved.
    documented: bool

    def as_dict(self) -> dict:
        return {
            "tables": [t.as_dict() for t in self.tables],
            "built_at": self.built_at,
            "build_ms": round(self.build_ms, 1),
            "lake": self.lake,
            "documented": self.documented,
            "columns_total": sum(len(t.columns) for t in self.tables),
            "columns_described": sum(
                1 for t in self.tables for c in t.columns if c.description
            ),
        }


# Which Dagster asset writes each layer's objects, as key path segments.
#
# Not derived from the layer, because it is not derivable. The curated and mart
# objects are written by the publish multi-assets under `lake/<layer>/<table>`; the
# raw objects are written by the ingest asset for their source, whose key is just its
# function name. Guessing `raw/<table>` would produce a link to the *warehouse* table
# of that name, which is a different asset that happens to share it.
#
# Every other source lands several tables from one shared ingest asset -- camera's
# six exports all come out of one call to the export API -- so one name per source
# is enough. `ai_platform` is the exception: `homelab_datalake.assets.ai_platform`
# builds one asset per table from the registry, so its entry is a function rather
# than a name, resolved per dataset below instead of listed fifty-two times here.
_RAW_INGEST_ASSET = {
    "npm": "npm_access_raw",
    "litellm": "litellm_spend_raw",
    "jellyfin": "jellyfin_library_raw",
    "camera": "camera_events_raw",
    "tmdb": "tmdb_titles_raw",
    "prometheus": "prometheus_samples_raw",
    "status_page": "status_page_raw",
    "host": "host_logs_raw",
    "keycloak": "keycloak_raw",
    "radarr": "radarr_raw",
    "sonarr": "sonarr_raw",
    "jellyseerr": "jellyseerr_raw",
    "banking_tracker": "finance_raw",
    "banking_tracker_db": "finance_raw",
    "infra_control": "infra_control_raw",
}


def asset_path(dataset: layout.Dataset) -> tuple[str, ...]:
    if dataset.layer == layout.LAYER_RAW:
        if dataset.source == "ai_platform":
            return (f"{dataset.table}_raw",)
        return (_RAW_INGEST_ASSET[dataset.source],)
    return ("lake", dataset.layer, dataset.table)


# --------------------------------------------------------------------------------
# the dbt manifest
# --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Documentation:
    """What dbt says about one table."""

    description: str
    columns: dict[str, str]


def read_manifest(path: str = DEFAULT_MANIFEST_PATH) -> dict[str, Documentation]:
    """Table name -> its prose, from `dbt parse`'s manifest.

    Sources and models are both read and both keyed by their table name, because
    that is the name the lake uses. dbt keys them by unique_id
    (`source.<package>.raw.npm_access`), which carries a package name this side of
    the join has no reason to know.

    A missing or unreadable manifest returns nothing rather than raising. The
    catalogue's job is to say what exists; it can do that without the prose, and an
    API that refuses to answer because the documentation is missing is worse than one
    that answers with blanks.
    """
    try:
        with open(path, encoding="utf-8") as handle:
            manifest = json.load(handle)
    except (OSError, ValueError) as error:
        logger.warning("no dbt manifest at %s (%s); descriptions will be blank", path, error)
        return {}

    docs: dict[str, Documentation] = {}
    for group in ("sources", "nodes"):
        for node in manifest.get(group, {}).values():
            if node.get("resource_type") not in ("source", "model"):
                continue
            name = node.get("name")
            if not name:
                continue
            if name in docs:
                # Two dbt nodes claiming one lake table. Not fatal, but the second
                # would silently win, so it says so.
                logger.warning("two dbt nodes are called %r; keeping the first", name)
                continue
            docs[name] = Documentation(
                description=_clean(node.get("description")),
                columns={
                    column_name: _clean(column.get("description"))
                    for column_name, column in (node.get("columns") or {}).items()
                },
            )
    return docs


def _clean(text: str | None) -> str:
    """dbt's block scalars arrive with the YAML line wrapping still in them."""
    return " ".join((text or "").split())


# --------------------------------------------------------------------------------
# the Parquet footers
# --------------------------------------------------------------------------------


def _footer_schema(con: duckdb.DuckDBPyConnection, paths: list[str]) -> list[tuple[str, str]]:
    """(name, type) for every column across a dataset's objects.

    `union_by_name` because a partition written before a column existed is a
    supported state -- the warehouse loader tolerates it, so the catalogue must
    describe the union rather than whichever object DuckDB happened to open first.
    """
    rows = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)", [paths]
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


def _footer_rows(con: duckdb.DuckDBPyConnection, paths: list[str]) -> int:
    """Rows across the dataset, from the footers alone.

    Explicit object paths avoid repeating S3 glob listings and exclude attachments.
    No data pages are read to obtain the count.
    """
    row = con.execute(
        "SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata(?)", [paths]
    ).fetchone()
    return int(row[0]) if row else 0


# Eight concurrent footer readers, with one DuckDB execution thread each. This bounds
# memory/CPU and S3 concurrency independently of the number of registered datasets.
_WORKERS = 8


@dataclass(frozen=True)
class Object:
    key: str
    size: int
    identity: str


def _objects_by_prefix(lake: LakeStorage) -> dict[str, tuple[Object, ...]]:
    """List registered prefixes, never inboxes, image payloads or other datasets."""
    client = lake._client() if lake.uses_s3 else None

    def listed(dataset):
        objects = []
        if client is not None:
            pages = client.get_paginator("list_objects_v2")
            for page in pages.paginate(Bucket=lake.bucket, Prefix=dataset.prefix + "/"):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        identity = str((obj.get("ETag"), obj.get("LastModified")))
                        objects.append(Object(obj["Key"], obj["Size"], identity))
        else:
            root = Path(lake.local_root)
            for path in (root / dataset.prefix).rglob("*.parquet"):
                if path.is_file():
                    stat = path.stat()
                    identity = str((stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns))
                    objects.append(Object(str(path.relative_to(root)), stat.st_size, identity))
        return dataset.prefix, tuple(sorted(objects, key=lambda obj: obj.key))

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        return dict(pool.map(listed, layout.ALL_DATASETS))


def _fingerprint(lake: LakeStorage, objects: tuple[Object, ...]) -> str:
    # The endpoint is part of the namespace: two S3 servers may use the same bucket.
    values = [lake.endpoint, lake.describe(), [(o.key, o.size, o.identity) for o in objects]]
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def _load_metadata(path: str | None) -> dict:
    if not path:
        return {}
    try:
        value = json.loads(Path(path).read_text())
        entries = value["entries"]
        if value["version"] != 2 or not isinstance(entries, dict):
            return {}
        schemas = value["schemas"]
        for entry in entries.values():
            entry["schema"] = schemas[entry["schema"]]
        for entry in entries.values():
            if (not isinstance(entry["rows"], int) or entry["rows"] < 0
                    or not isinstance(entry["schema"], list)
                    or any(not isinstance(c, list) or len(c) != 2
                           or any(not isinstance(v, str) for v in c) for c in entry["schema"])):
                return {}
        return entries
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        logger.warning("catalogue metadata cache missing or invalid; reading real footers")
        return {}


def _save_metadata(path: str | None, entries: dict) -> None:
    if not path:
        return
    temporary = None
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=target.name + ".", dir=target.parent)
        # Thousands of daily objects usually share a handful of schemas. Store
        # each schema once, rather than multiplying a wide column list per object.
        schemas = {}
        compact = {}
        for key, entry in entries.items():
            schema = entry["schema"]
            schema_key = hashlib.sha256(json.dumps(schema).encode()).hexdigest()
            schemas[schema_key] = schema
            compact[key] = {"schema": schema_key, "rows": entry["rows"]}
        with os.fdopen(fd, "w") as handle:
            json.dump({"version": 2, "schemas": schemas, "entries": compact}, handle)
        os.replace(temporary, target)
    except OSError:
        # The cache is an optimization. A read-only/unavailable cache must not
        # hide truthful results obtained from the lake.
        logger.warning("could not persist catalogue metadata cache", exc_info=True)
    finally:
        if temporary:
            Path(temporary).unlink(missing_ok=True)


# --------------------------------------------------------------------------------
# building the catalogue
# --------------------------------------------------------------------------------


def build(lake: LakeStorage | None = None, manifest_path: str = DEFAULT_MANIFEST_PATH,
          metadata_cache_path: str | None = None) -> Catalogue:
    """Join real Parquet metadata to current documentation, reusing unchanged results."""
    started = time.monotonic()
    lake = lake or LakeStorage.from_env()
    docs = read_manifest(manifest_path)
    objects = _objects_by_prefix(lake)
    cache_path = metadata_cache_path or os.environ.get("DATALAKE_CATALOGUE_CACHE_PATH")
    previous = _load_metadata(cache_path)
    local = threading.local()
    connections = []
    lock = threading.Lock()

    def connection():
        if not hasattr(local, "con"):
            local.con = duckdb.connect(":memory:", config={"threads": 1, "TimeZone": "UTC"})
            with lock:
                connections.append(local.con)
            lake.configure_duckdb(local.con)
        return local.con

    def inspect(obj):
        fingerprint = _fingerprint(lake, (obj,))
        cached = previous.get(fingerprint)
        if cached is None:
            path = (f"s3://{lake.bucket}/{obj.key}" if lake.uses_s3
                    else str(Path(lake.local_root).resolve() / obj.key))
            try:
                con = connection()
                cached = {"schema": _footer_schema(con, [path]), "rows": _footer_rows(con, [path])}
            except duckdb.Error as error:
                logger.warning("cannot read footer for %s: %s", obj.key, error)
                return obj.key, fingerprint, None, str(error)
        return obj.key, fingerprint, cached, None

    try:
        with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
            results = list(pool.map(inspect, (obj for group in objects.values() for obj in group)))
        metadata = {key: (entry, error) for key, _, entry, error in results}
        # Schema union is a local type-binding operation over distinct observed
        # schemas. DuckDB applies the same name/type promotion rules as the lake's
        # read_parquet(..., union_by_name=true), without rereading old S3 footers.
        union = duckdb.connect(":memory:", config={"threads": 1})
        try:
            tables = []
            for dataset in layout.ALL_DATASETS:
                landed = objects[dataset.prefix]
                entries = [metadata[obj.key] for obj in landed]
                error = next((error for _, error in entries if error is not None), None)
                summary = None
                if landed and error is None:
                    summary = {"schema": _union_schema(union, [m["schema"] for m, _ in entries]),
                               "rows": sum(m["rows"] for m, _ in entries)}
                tables.append(_describe(dataset, docs, landed, summary, error))
        finally:
            union.close()
    finally:
        for con in connections:
            con.close()
    _save_metadata(cache_path, {key: entry for _, key, entry, _ in results if entry is not None})
    return Catalogue(
        tables=tuple(tables),
        built_at=time.time(),
        build_ms=(time.monotonic() - started) * 1000,
        lake=lake.describe(),
        documented=bool(docs),
    )


def _union_schema(con, schemas) -> list[tuple[str, str]]:
    distinct = list(dict.fromkeys(tuple(tuple(c) for c in schema) for schema in schemas))
    if len(distinct) == 1:
        return list(distinct[0])
    selects = []
    for schema in distinct:
        # Names and type spellings were obtained from DuckDB's DESCRIBE, never
        # from documentation or request input. Quote names just as DuckDB does.
        columns = [f'CAST(NULL AS {kind}) AS "{name.replace(chr(34), chr(34) * 2)}"'
                   for name, kind in schema]
        selects.append("SELECT " + ", ".join(columns) + " WHERE false")
    rows = con.execute("DESCRIBE " + " UNION ALL BY NAME ".join(selects)).fetchall()
    return [(row[0], row[1]) for row in rows]


def _describe(dataset, docs, objects, metadata, error: str | None = None) -> Table:
    size = sum(o.size for o in objects)
    documentation = docs.get(dataset.table, Documentation("", {}))
    # The dataset's own description beats the manifest's. It is the one a reader
    # sees in the Dagster graph and in the lake layout, and having the tree and the
    # graph disagree about what a dataset is would be worse than either alone.
    description = dataset.description or documentation.description

    common = {
        "table": dataset.table,
        "qualified_name": dataset.qualified_name,
        "layer": dataset.layer,
        "domain": dataset.domain,
        "source": dataset.source,
        "prefix": dataset.prefix,
        "description": description,
        "partition_column": dataset.partition_column,
        "objects": len(objects),
        "bytes": size,
        "asset_path": asset_path(dataset),
    }

    if not objects:
        # No footers to read. Asking DuckDB anyway costs a round trip and returns an
        # IOException that says less than this does.
        return Table(**common, columns=(), rows=0,
                     unavailable="no objects have landed under this prefix yet")

    if error is not None:
        return Table(**common, columns=(), rows=0, unavailable=error)
    schema = metadata["schema"]
    rows = metadata["rows"]

    described = documentation.columns
    for name in described:
        if name not in {column for column, _ in schema}:
            # Documentation for a column that is not in the data. Harmless to the
            # caller and a real signal to whoever wrote it: usually a rename that
            # only got done on one side.
            logger.info("%s documents %r, which is not in the Parquet", dataset.table, name)

    return Table(
        **common,
        columns=tuple(
            Column(name=name, type=type_name, description=described.get(name, ""))
            for name, type_name in schema
        ),
        rows=rows,
    )


class CatalogueCache:
    """One built catalogue, shared by every request, rebuilt on demand.

    The lock is held across the build rather than only around the assignment. That
    makes twenty simultaneous first requests wait for one build instead of starting
    twenty against MinIO, which is the failure this cache exists to prevent -- a cold
    cache under load is exactly when the expensive path must not be taken twice.
    """

    def __init__(
        self,
        ttl: float = DEFAULT_TTL_SECONDS,
        builder=build,
        stamp=publish_stamp,
    ) -> None:
        self._ttl = ttl
        self._builder = builder
        self._stamp = stamp
        self._lock = threading.Lock()
        self._value: Catalogue | None = None
        self._built_at = 0.0
        self._stamped: tuple[int, int] | None = None

    def get(self) -> Catalogue:
        with self._lock:
            if self._value is None or self._stale():
                self._rebuild()
            return self._value

    def _stale(self) -> bool:
        stamp = self._stamp()
        # A stamp that has appeared, changed or vanished all mean the same thing:
        # what is underneath is not what this was built from.
        if stamp != self._stamped:
            logger.info("the lake was republished (%s -> %s); rebuilding the catalogue",
                        self._stamped, stamp)
            return True
        return (time.monotonic() - self._built_at) > self._ttl

    def refresh(self) -> Catalogue:
        with self._lock:
            self._rebuild()
            return self._value

    def _rebuild(self) -> None:
        stamped = self._stamp()
        self._value = self._builder()
        self._built_at = time.monotonic()
        self._stamped = stamped
        logger.info(
            "catalogue built: %d datasets, %d columns, %.0f ms",
            len(self._value.tables),
            sum(len(t.columns) for t in self._value.tables),
            self._value.build_ms,
        )
