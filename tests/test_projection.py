"""project(): what a row already carries in `_unmapped` is kept, not overwritten.

The Jellyfin snapshots were losing every leftover field this way until 2026-09-19 --
see the section on `_unmapped` in test_jellyfin.py for what it cost.
"""

import json

import pyarrow as pa

from homelab_datalake.projection import project

SCHEMA = pa.schema([("a", pa.string()), ("_unmapped", pa.string())])


def _unmapped(row):
    value = row["_unmapped"]
    return None if value is None else json.loads(value)


def test_leftovers_a_caller_already_computed_are_kept():
    rows, seen = project([{"a": "x", "_unmapped": json.dumps({"Old": 1})}], SCHEMA)
    assert _unmapped(rows[0]) == {"Old": 1}
    assert seen == {"Old"}


def test_carried_and_found_leftovers_are_merged():
    rows, seen = project([{"a": "x", "_unmapped": {"Old": 1}, "New": 2}], SCHEMA)
    assert _unmapped(rows[0]) == {"Old": 1, "New": 2}
    assert seen == {"Old", "New"}


def test_this_pass_wins_a_clash_since_it_is_the_newer_reading():
    rows, _ = project([{"a": "x", "_unmapped": {"K": "old"}, "K": "new"}], SCHEMA)
    assert _unmapped(rows[0]) == {"K": "new"}


def test_a_carried_value_that_is_not_json_is_kept_rather_than_dropped():
    rows, _ = project([{"a": "x", "_unmapped": "not json"}], SCHEMA)
    assert _unmapped(rows[0]) == {"_unmapped": "not json"}


def test_nothing_carried_and_nothing_found_is_still_null():
    rows, seen = project([{"a": "x", "_unmapped": None}], SCHEMA)
    assert rows[0]["_unmapped"] is None
    assert seen == set()
