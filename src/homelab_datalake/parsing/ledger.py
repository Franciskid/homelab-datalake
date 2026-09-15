"""A table's history, kept as every distinct version of every row.

For sources whose tables are *state* rather than *events* -- Radarr's movies, a
Jellyseerr request's status, Keycloak's users -- a daily snapshot would copy tens of
thousands of unchanged rows every day, and replacing a snapshot would forget every row
the source has since deleted. A ledger does neither.

Each run reads the whole table and merges it into one object:

* a row whose content has not changed only has its `_last_seen_at` moved forward;
* a row whose content changed gets a new version, with its own `_first_seen_at`;
* a row the source no longer has keeps its last version, and its `_last_seen_at`
  simply stops advancing.

So the latest version of a key is the one with the greatest `_last_seen_at`, a key is
gone from the source when that is older than the ledger's newest `_last_seen_at`, and a
request's path from pending to available is its versions in order. Nothing is ever
removed, which is the only safe behaviour for a lake that is sometimes the last copy.

**Volatile columns** are those that change on every sync without meaning anything --
Radarr's `LastSearchTime`, a popularity score. They are left out of the version hash
and overwritten with the newest value, so a nightly search does not mint 21 000 new
episode versions.

Every source value is stored as text. SQLite's column types are advisory, so a column
declared INTEGER may hold anything; text is the one type every row agrees on, and the
staging models cast.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

VERSION_HASH = "_version_hash"
FIRST_SEEN = "_first_seen_at"
LAST_SEEN = "_last_seen_at"
META_COLUMNS = (VERSION_HASH, FIRST_SEEN, LAST_SEEN)

META_SCHEMA = pa.schema([
    (VERSION_HASH, pa.string()),
    (FIRST_SEEN, pa.timestamp("us", tz="UTC")),
    (LAST_SEEN, pa.timestamp("us", tz="UTC")),
])

# Query-string parameters that are credentials. Radarr and Sonarr store each release's
# download URL and indexer GUID in their history, and both carry the indexer's API key
# (`jackett_apikey=`, `apikey=`). Those are not personal data; they are keys that would
# let anyone who can read the lake download through the indexer, so they never land.
# `&` is how the JSON in those columns escapes `&`.
_SECRET_PARAMETER = re.compile(
    r"((?:[?&]|\\u0026)(?:jackett_apikey|apikey|api_key|passkey|authkey|torrent_pass|token)=)"
    r"[^&\"\s\\]+",
    re.IGNORECASE,
)
REDACTED = "<redacted>"


def redact_secrets(value: str | None) -> str | None:
    if value is None:
        return None
    return _SECRET_PARAMETER.sub(lambda m: m.group(1) + REDACTED, value)


def as_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def version_hash(row: dict[str, str | None], volatile: frozenset[str]) -> str:
    stable = {k: v for k, v in row.items() if k not in volatile and k not in META_COLUMNS}
    canonical = json.dumps(sorted(stable.items()), separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass
class MergeStats:
    versions_before: int
    versions_after: int
    new_versions: int
    current_keys: int
    keys_gone_from_source: int


def merge(
    existing: pa.Table | None,
    rows: list[dict],
    key_columns: tuple[str, ...],
    captured_at: datetime,
    volatile: frozenset[str] = frozenset(),
) -> tuple[pa.Table, MergeStats]:
    """Merge one full read of a source table into its ledger."""
    source_columns: list[str] = []
    old_rows: list[dict] = []
    if existing is not None and existing.num_rows:
        source_columns = [c for c in existing.column_names if c not in META_COLUMNS]
        old_rows = existing.to_pylist()
    for row in rows:
        for column in row:
            if column not in source_columns:
                source_columns.append(column)

    def key_of(row: dict) -> tuple:
        return tuple(row.get(column) for column in key_columns) if key_columns else ()

    index: dict[tuple, dict] = {}
    for row in old_rows:
        index[(key_of(row), row[VERSION_HASH])] = row

    current_keys = set()
    new_versions = 0
    for raw in rows:
        row = {column: as_text(raw.get(column)) for column in source_columns}
        digest = version_hash(row, volatile)
        key = key_of(row)
        # Without a primary key a row *is* its content, so distinct contents are the keys.
        current_keys.add(key if key_columns else digest)
        incumbent = index.get((key, digest))
        if incumbent is None:
            index[(key, digest)] = {**row, VERSION_HASH: digest, FIRST_SEEN: captured_at, LAST_SEEN: captured_at}
            new_versions += 1
        else:
            for column in volatile:
                if column in row:
                    incumbent[column] = row[column]
            incumbent[LAST_SEEN] = captured_at

    all_keys = {key for key, _digest in index}
    merged = sorted(
        index.values(),
        key=lambda r: (tuple("" if v is None else v for v in key_of(r)), r[FIRST_SEEN]),
    )
    table = to_table(merged, source_columns)
    if table.num_rows < len(old_rows):
        raise RuntimeError(
            f"ledger merge would shrink {len(old_rows)} versions to {table.num_rows}; refusing"
        )
    return table, MergeStats(
        versions_before=len(old_rows),
        versions_after=table.num_rows,
        new_versions=new_versions,
        current_keys=len(current_keys),
        keys_gone_from_source=len(all_keys - current_keys) if key_columns else 0,
    )


def to_table(rows: list[dict], source_columns: list[str]) -> pa.Table:
    schema = pa.schema([(c, pa.string()) for c in source_columns] + list(META_SCHEMA))
    return pa.Table.from_pylist(
        [{c: row.get(c) for c in schema.names} for row in rows], schema=schema
    )
