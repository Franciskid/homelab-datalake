"""Reading one partition of the AI platform's own database.

Same shape as `litellm_spend.py`: pure functions, rows injectable so tests never
need a database, psycopg reached only when `rows=None`. What is different here is
that one module now serves 52 tables instead of one -- so there is a single
extraction function, driven entirely by the `TableSpec` from
`ai_platform_registry.py`, rather than fifty hand-written queries.

**One connection per call, and it is closed before the function returns.** The
`datalake_ro` role on this Postgres carries `CONNECTION LIMIT 3` -- see
`infra/ai-platform-datalake-role.sql` -- on a server whose `max_connections` is 100
and which Keycloak, the banking tracker, LiteLLM and Dagster's own storage all share.
`psycopg.connect(...)` is used as a context manager and never held between calls, the
same discipline `litellm_spend.py` already follows; nothing here pools connections
across the 52 tables' worth of asset runs a day's schedule fires.

**Session timezone is pinned to UTC on every connection**, the same reason
`homelab_datalake.assets.warehouse.connect()` pins DuckDB's: `timestamp with time
zone` columns come back from psycopg already tz-aware, but *which* zone they are
rendered in depends on the session setting, and comparing an unpinned session's
`started_at` against a UTC day boundary would silently shift near midnight depending
on who -- or what container -- happened to run the query.

## Two shapes of row, and one shape of frame

Every ordinary column lands through `_coerce`, which maps a Postgres value onto the
Arrow type `ai_platform_types.arrow_type_for` says it should be: a `jsonb` value
(already a Python `dict`/`list` once psycopg has decoded it) is dumped to JSON text,
a `uuid.UUID` becomes its string form, and everything else passes through unchanged
provided its Python type already matches -- there is no column-by-column special
case beyond that.

Two column kinds need more than that:

* **A binary column** (`generated_files.data`, `message_attachments.data`) is read --
  the bytes have to be fetched to be written anywhere -- but never lands in the row.
  `extract_table_day` computes a deterministic `object_key` for it
  (`<prefix>/dt=<day>/<id>`) and returns the raw bytes in `_binary_data`, a
  transient key the caller (the Dagster asset, which alone touches the lake) pops
  before building the Arrow table, writes the object under that key, and then
  discards. Deterministic keys are what makes a re-run of a partition idempotent --
  see the module-level docstring in `assets/ai_platform.py`.
* **`usage_events.metadata['images']`** -- see `extract_frames` below. Each valid
  data URI becomes one decoded frame under a deterministic key; the row keeps
  `frame_keys` (in order), `frame_count`, `frame_bytes`, and a `metadata` value with
  `images` removed and every other field -- `clipUrl` included -- intact.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta

import pyarrow as pa

from homelab_datalake.parsing.ai_platform_registry import MODE_WINDOW, TableSpec
from homelab_datalake.parsing.ai_platform_types import arrow_type_for
from homelab_datalake.privacy import strip_query

# Where the binary payloads and decoded frames are written. Not under raw/curated/
# marts -- these are opaque objects referenced *by* a Parquet row's `object_key` or
# `frame_keys`, not a queryable dataset in their own right, so they sit in their own
# top-level area rather than pretending to be a fourth medallion layer.
GATEWAY_FRAMES_PREFIX = "files/ai_platform/gateway_frames"

BINARY_PREFIXES: dict[str, str] = {
    "message_attachments": "files/ai_platform/message_attachments",
    "generated_files": "files/ai_platform/generated_files",
}


def connection_settings() -> dict:
    return {
        "host": os.environ.get("AI_PLATFORM_PG_HOST", "localhost"),
        "port": int(os.environ.get("AI_PLATFORM_PG_PORT", "5432")),
        "dbname": os.environ.get("AI_PLATFORM_PG_DB", "ai_platform"),
        "user": os.environ.get("AI_PLATFORM_PG_USER", ""),
        "password": os.environ.get("AI_PLATFORM_PG_PASSWORD", ""),
    }


def _pk_column(spec: TableSpec) -> str:
    """The first column of the (possibly composite) primary key.

    Used only to order a query deterministically and to name an object -- never to
    assert uniqueness, which a composite key would need both halves for and neither
    binary column nor frame-bearing table has one.
    """
    return spec.primary_key.split(",")[0]


def build_query(spec: TableSpec) -> str:
    """The SELECT for one table, built from its registry entry alone.

    Column names are identifiers from `TableSpec.columns` -- module constants, never
    input, the same guarantee `litellm_spend.py`'s `_build_query` documents. The only
    values reaching the database are the bound `%(start)s`/`%(end)s` parameters.
    """
    columns = ", ".join(f'"{c}"' for c in spec.columns)
    order_column = _pk_column(spec)
    if spec.mode == MODE_WINDOW:
        return (
            f'SELECT {columns} FROM "{spec.name}" '  # noqa: S608
            f'WHERE "{spec.change_column}" >= %(start)s AND "{spec.change_column}" < %(end)s '
            f'ORDER BY "{spec.change_column}", "{order_column}"'
        )
    return f'SELECT {columns} FROM "{spec.name}" ORDER BY "{order_column}"'  # noqa: S608


def _fetch(spec: TableSpec, params: dict, rows: list[dict] | None) -> list[dict]:
    if rows is not None:
        return rows

    import psycopg

    query = build_query(spec)
    with psycopg.connect(**connection_settings()) as conn:
        conn.execute("SET TIME ZONE 'UTC';")
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [c.name for c in cur.description]
            return [dict(zip(columns, record, strict=True)) for record in cur.fetchall()]


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _coerce(value, arrow_type: pa.DataType):
    """One Postgres value, onto the Arrow type it is landing as.

    A `jsonb` column arrives from psycopg as a Python `dict` or `list` already
    decoded -- so any dict/list value is JSON, regardless of which column it came
    from, and is dumped to text unconditionally. That is what lets one function serve
    every jsonb column in the registry without naming each one.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, sort_keys=True)
    if pa.types.is_timestamp(arrow_type):
        return _as_utc(value)
    if pa.types.is_string(arrow_type):
        return str(value) if isinstance(value, uuid.UUID) else value
    if pa.types.is_floating(arrow_type):
        return float(value)
    if pa.types.is_integer(arrow_type):
        return int(value)
    if pa.types.is_boolean(arrow_type):
        return bool(value)
    return value


# --------------------------------------------------------------------------------
# Frames: usage_events.metadata['images'], one data URI per camera frame attached
# to a gateway call.
# --------------------------------------------------------------------------------

_DATA_URI_RE = re.compile(
    r"^data:image/(?P<subtype>[a-zA-Z0-9.+-]+);base64,(?P<payload>.+)$", re.DOTALL
)


@dataclass(frozen=True)
class DecodedFrame:
    data: bytes
    content_type: str


def decode_data_uri(value) -> DecodedFrame | None:
    """One `data:image/...;base64,...` string, decoded -- or `None` if it is not one.

    Never raises. A frame that is not a string, does not start with `data:image/`,
    does not match the expected shape, or does not base64-decode is simply not a
    frame -- the caller counts these rather than the day's extraction failing over
    one bad entry in a list of two hundred.
    """
    if not isinstance(value, str) or not value.startswith("data:image/"):
        return None
    match = _DATA_URI_RE.match(value)
    if not match:
        return None
    try:
        raw = base64.b64decode(match.group("payload"), validate=True)
    except (binascii.Error, ValueError):
        return None
    if not raw:
        return None
    return DecodedFrame(data=raw, content_type=f"image/{match.group('subtype')}")


def extract_frames(metadata) -> tuple[list[DecodedFrame], int, dict]:
    """(decoded frames in order, how many were skipped, metadata with `images` gone).

    Every other key of `metadata` survives untouched -- `clipUrl` included, per the
    owner's decision that a usage_event's metadata is landed whole except for the
    base64 payloads themselves, which are large, opaque to SQL, and belong in the
    lake as objects rather than as text in a warehouse column.
    """
    if not isinstance(metadata, dict):
        return [], 0, {}
    images = metadata.get("images")
    remaining = {k: v for k, v in metadata.items() if k != "images"}
    if not isinstance(images, list):
        return [], 0, remaining
    frames: list[DecodedFrame] = []
    skipped = 0
    for item in images:
        decoded = decode_data_uri(item)
        if decoded is None:
            skipped += 1
            continue
        frames.append(decoded)
    return frames, skipped, remaining


def _frame_keys(day: date, usage_event_id: str, count: int) -> list[str]:
    """Deterministic object keys for a row's frames, in order.

    Deterministic in `day` and `usage_event_id` alone -- not a UUID minted per
    write -- so re-running the same partition writes the same `count` objects back
    to the same keys rather than accumulating a new set beside the old ones every
    time the day is re-extracted inside its trailing window.
    """
    return [
        f"{GATEWAY_FRAMES_PREFIX}/dt={day.isoformat()}/{usage_event_id}/{n}.jpg"
        for n in range(count)
    ]


# --------------------------------------------------------------------------------
# The one extraction function every table in the registry goes through.
# --------------------------------------------------------------------------------


@dataclass
class ExtractResult:
    rows: list[dict] = field(default_factory=list)
    count: int = 0
    # Only ever non-zero for usage_events. Surfaced on the asset's Output metadata,
    # never raised: a malformed data URI is a fact about one frame, not about the day.
    frame_decode_errors: int = 0


def extract_table_day(spec: TableSpec, day: date, rows: list[dict] | None = None) -> ExtractResult:
    """One partition's worth of `spec`.

    For `MODE_WINDOW` this is the rows whose `change_column` falls on `day`, UTC.
    For `MODE_SNAPSHOT` it is the whole table, landed under `day`'s partition --
    there is no filter to apply, by construction (see `TableSpec.__post_init__`).

    `rows` is injectable, in place of the Postgres fetch, so every table's shape is
    testable with no database -- the same contract `litellm_spend.extract_day` makes.
    Injected rows are dicts keyed exactly as `spec.columns` names them, with Python
    values shaped the way psycopg would hand them back (a `dict`/`list` for jsonb, a
    `uuid.UUID` for uuid, a tz-aware `datetime` for timestamptz, `bytes` for bytea).
    """
    if spec.mode == MODE_WINDOW:
        start = datetime.combine(day, time.min, tzinfo=UTC)
        end = start + timedelta(days=1)
        params: dict = {"start": start, "end": end}
    else:
        params = {}

    raw_rows = _fetch(spec, params, rows)
    binary_prefix = BINARY_PREFIXES.get(spec.name)

    out = ExtractResult()
    for raw in raw_rows:
        row: dict = {"dt": day}
        binary_bytes = None
        frames: list[DecodedFrame] = []

        for column in spec.columns:
            value = raw.get(column)
            if column == spec.binary_column:
                binary_bytes = value
                continue
            if column in spec.redact_query_columns:
                value = strip_query(value)
            if spec.decode_metadata_images and column == "metadata":
                frames, skipped, value = extract_frames(value)
                out.frame_decode_errors += skipped
            row[column] = _coerce(value, arrow_type_for(spec.name, column))

        if spec.binary_column:
            pk_value = str(raw.get(_pk_column(spec)))
            if binary_bytes is not None:
                row["object_key"] = f"{binary_prefix}/dt={day.isoformat()}/{pk_value}"
                # Transient: popped by the asset, which alone writes to the lake, and
                # never reaches the Arrow table this row is eventually built into.
                row["_binary_data"] = bytes(binary_bytes)
            else:
                row["object_key"] = None

        if spec.decode_metadata_images:
            pk_value = str(raw.get(_pk_column(spec)))
            row["frame_keys"] = _frame_keys(day, pk_value, len(frames))
            row["frame_count"] = len(frames)
            row["frame_bytes"] = sum(len(f.data) for f in frames)
            # Transient, same reason as `_binary_data`.
            row["_frames"] = frames

        out.rows.append(row)
        out.count += 1

    return out


__all__ = [
    "BINARY_PREFIXES",
    "GATEWAY_FRAMES_PREFIX",
    "DecodedFrame",
    "ExtractResult",
    "build_query",
    "connection_settings",
    "decode_data_uri",
    "extract_frames",
    "extract_table_day",
]
