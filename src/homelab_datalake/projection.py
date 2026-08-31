"""Fitting a source's rows to a declared schema without losing what it did not declare.

Every ingest asset in this repo projects the source's rows through a PyArrow schema
before writing them. That projection is deliberate -- it is what stops an upstream
change from silently widening the warehouse, including one that starts sending
something nobody meant to store.

What it must not be is *silent*. The camera export has sent `counts` on every event
since before this pipeline existed; because the schema did not name it, every row was
written without it and nothing anywhere said so. A field that arrives and is dropped
looked exactly like a field that never existed, so the loss was invisible for a month
and was only found by diffing the export against the lake by hand.

So the bargain here is: the column set stays fixed and reviewable, and anything
outside it is kept as JSON in `_unmapped` and *named* to the caller, which turns it
into an asset check that says "the source is sending you something you are not
storing" rather than nothing at all.

Nested values are picked field by field for the same reason in miniature: PyArrow
raises on a key a struct does not declare, so an upstream addition inside a nested
object would otherwise fail the whole partition rather than land it.
"""

from __future__ import annotations

import json

import pyarrow as pa

UNMAPPED_COLUMN = "_unmapped"


def coerce(value, dtype: pa.DataType, path: str, leftovers: dict):
    """Fit one value to its declared Arrow type, recording anything that does not fit."""
    if value is None:
        return None
    if pa.types.is_struct(dtype):
        if not isinstance(value, dict):
            leftovers[path] = value
            return None
        declared = {f.name for f in dtype}
        for key in value:
            if key not in declared:
                leftovers[f"{path}.{key}"] = value[key]
        return {
            f.name: coerce(value.get(f.name), f.type, f"{path}.{f.name}", leftovers)
            for f in dtype
        }
    if pa.types.is_list(dtype):
        if not isinstance(value, list):
            leftovers[path] = value
            return None
        return [coerce(item, dtype.value_type, f"{path}[]", leftovers) for item in value]
    return value


def project(
    rows: list[dict],
    schema: pa.Schema,
    json_columns: frozenset[str] | set[str] = frozenset(),
    ignore: frozenset[str] | set[str] = frozenset(),
) -> tuple[list[dict], set[str]]:
    """Fit rows to `schema`, keeping whatever did not fit rather than dropping it.

    `json_columns` name declared columns whose value arrives as an object and is
    stored as JSON text -- for a block that is rare enough not to earn a struct.

    `ignore` names source fields that are deliberately not stored and must not be
    reported as unmapped either: a filesystem path, a prompt payload, prose somebody
    wrote. The distinction matters. `_unmapped` is a prompt to give a field a column,
    and a field nobody will ever store would make that prompt permanent noise --
    which is how a warning stops being read.

    Returns the rows plus the set of field paths that went into `_unmapped`.
    """
    fields = {f.name: f.type for f in schema}
    names = [f.name for f in schema]
    unmapped_seen: set[str] = set()
    out = []

    for row in rows:
        leftovers: dict = {}
        built = {}
        for name in names:
            if name == UNMAPPED_COLUMN:
                continue
            value = row.get(name)
            if name in json_columns and value is not None and not isinstance(value, str):
                built[name] = json.dumps(value, default=str, sort_keys=True)
                continue
            built[name] = coerce(value, fields[name], name, leftovers)

        for key, value in row.items():
            if key not in fields and key not in ignore:
                leftovers[key] = value

        if UNMAPPED_COLUMN in fields:
            built[UNMAPPED_COLUMN] = (
                json.dumps(leftovers, default=str, sort_keys=True) if leftovers else None
            )
        unmapped_seen.update(leftovers)
        out.append(built)

    return out, unmapped_seen


def drop_keys(value: dict, *keys: str) -> dict:
    """A copy of `value` without `keys`. For trimming a nested entry before it is kept."""
    return {k: v for k, v in (value or {}).items() if k not in keys}
