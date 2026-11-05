"""analytics_hourly redoes only what changed, and never skips what it must not.

The failure these guard against is silent: a skipped reload leaves a mart looking fine
and holding yesterday's numbers. So most of this file is about when work is NOT skipped.
"""

import datetime as dt
import json

import duckdb
import pyarrow as pa
import pytest

from homelab_datalake import rebuild_state, serving
from homelab_datalake.resources import LakeStorage
from tests.conftest import build_marts

NOON = dt.datetime(2026, 11, 5, 12, 55, tzinfo=dt.UTC)
NIGHT = dt.datetime(2026, 11, 6, 3, 55, tzinfo=dt.UTC)
BEFORE_NIGHT = dt.datetime(2026, 11, 6, 2, 55, tzinfo=dt.UTC)


@pytest.fixture
def path(tmp_path):
    return str(tmp_path / "datalake.duckdb")


def _settled(path, code="v1", now=NOON):
    """A warehouse whose sources are loaded and built, and whose day's rebuild is done."""
    con = duckdb.connect(path)
    rebuild_state.begin_load(con, code=code, now=now)
    rebuild_state.record_loaded(con, "a", "fp-a")
    rebuild_state.record_loaded(con, "b", "fp-b")
    con.close()
    rebuild_state.record_built(path, marts_changed=True)


def _begin(path, **kwargs):
    con = duckdb.connect(path)
    try:
        return rebuild_state.begin_load(con, **{"code": "v1", "now": NOON, **kwargs})
    finally:
        con.close()


def _loaded(path):
    con = duckdb.connect(path)
    try:
        return rebuild_state.loaded(con)
    finally:
        con.close()


def test_a_warehouse_with_no_state_gets_a_full_build(path):
    duckdb.connect(path).close()
    assert rebuild_state.pending(path).full


def test_the_same_hour_with_nothing_changed_has_nothing_to_do(path):
    _settled(path)
    assert _begin(path) is False
    assert rebuild_state.pending(path) == rebuild_state.Pending(full=False, sources=())


def test_a_changed_source_is_pending_and_the_others_are_not(path):
    _settled(path)
    _begin(path)
    con = duckdb.connect(path)
    rebuild_state.record_loaded(con, "a", "fp-a-2")
    con.close()
    assert rebuild_state.pending(path) == rebuild_state.Pending(full=False, sources=("a",))


def test_a_failed_dbt_build_is_retried_by_the_next_run(path):
    """record_built never ran, so the source stays pending however many hours pass."""
    _settled(path)
    con = duckdb.connect(path)
    rebuild_state.record_loaded(con, "a", "fp-a-2")
    con.close()
    assert _begin(path) is False
    assert rebuild_state.pending(path).sources == ("a",)
    rebuild_state.record_built(path, marts_changed=True)
    assert rebuild_state.pending(path).sources == ()


def test_new_code_rebuilds_everything(path):
    _settled(path)
    assert _begin(path, code="v2") is True
    assert _loaded(path) == {}
    assert rebuild_state.pending(path).full


def test_an_operator_can_force_a_full_rebuild(path):
    _settled(path)
    assert _begin(path, forced=True) is True
    assert rebuild_state.pending(path).full


def test_the_first_run_from_three_each_day_rebuilds_everything_and_only_that_one(path):
    _settled(path)
    assert _begin(path, now=BEFORE_NIGHT) is False
    assert _begin(path, now=NIGHT) is True
    rebuild_state.record_built(path, marts_changed=True)
    assert _begin(path, now=NIGHT + dt.timedelta(hours=1)) is False


def test_a_deploy_before_three_does_not_cancel_the_nights_rebuild(path):
    _settled(path)
    assert _begin(path, code="v2", now=BEFORE_NIGHT) is True
    rebuild_state.record_built(path, marts_changed=True)
    assert _begin(path, code="v2", now=NIGHT) is True


def test_a_run_that_dies_during_a_full_rebuild_is_finished_by_the_next_one(path):
    _settled(path)
    _begin(path, code="v2")
    con = duckdb.connect(path)
    rebuild_state.record_loaded(con, "a", "fp-a")  # then the process dies, before "b"
    con.close()
    assert _begin(path, code="v2") is False, "the decision was already taken and recorded"
    assert _loaded(path) == {"a": "fp-a"}, "b has no fingerprint, so it will be reloaded"
    assert rebuild_state.pending(path).full, "and dbt still builds everything"


def test_a_build_that_changed_no_mart_keeps_the_generation(path):
    _settled(path)
    before = rebuild_state.marts_generation(path)
    rebuild_state.record_built(path, marts_changed=False)
    assert rebuild_state.marts_generation(path) == before
    rebuild_state.record_built(path, marts_changed=True)
    assert rebuild_state.marts_generation(path) != before


def test_a_warehouse_from_before_this_has_no_generation(path):
    duckdb.connect(path).close()
    assert rebuild_state.marts_generation(path) is None


def _write(lake, key, rows):
    lake.write_table(key, pa.table({"a": rows}))


def test_the_fingerprint_follows_the_objects(tmp_path):
    lake = LakeStorage(local_root=str(tmp_path))
    empty = lake.parquet_fingerprint("raw/x")
    _write(lake, "raw/x/dt=1/data.parquet", [1])
    one = lake.parquet_fingerprint("raw/x")
    assert one != empty
    assert lake.parquet_fingerprint("raw/x") == one, "nothing changed, so nothing is reloaded"
    _write(lake, "raw/x/dt=2/data.parquet", [2])
    two = lake.parquet_fingerprint("raw/x")
    assert two != one, "a new partition"
    _write(lake, "raw/x/dt=1/data.parquet", [1, 3])
    assert lake.parquet_fingerprint("raw/x") != two, "a replaced partition"


def test_a_prefix_is_not_confused_with_one_it_starts_with(tmp_path):
    """raw/npm/access and raw/npm/access_rejects both exist."""
    lake = LakeStorage(local_root=str(tmp_path))
    _write(lake, "raw/access/data.parquet", [1])
    before = lake.parquet_fingerprint("raw/access")
    _write(lake, "raw/access_rejects/data.parquet", [1])
    assert lake.parquet_fingerprint("raw/access") == before


def test_a_bucket_that_does_not_answer_is_never_read_as_unchanged(tmp_path, monkeypatch):
    lake = LakeStorage(endpoint="http://127.0.0.1:1", bucket="b")

    class Refusing:
        def get_paginator(self, _name):
            raise ConnectionError("Temporary failure in name resolution")

    monkeypatch.setattr(LakeStorage, "_client", lambda self: Refusing())
    with pytest.raises(ConnectionError):
        lake.parquet_fingerprint("raw/x")


def test_a_changed_load_statement_changes_the_fingerprint():
    assert rebuild_state.fingerprint("objects", "one") != rebuild_state.fingerprint("objects", "two")


def _manifest(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({
        "sources": {
            "source.p.raw.read": {"name": "read", "source_name": "raw"},
            "source.p.raw.view_only": {"name": "view_only", "source_name": "raw"},
        },
        "child_map": {
            "source.p.raw.read": ["model.p.stg"],
            "model.p.stg": ["model.p.mart", "test.p.check"],
            "model.p.mart": [],
            "source.p.raw.view_only": ["model.p.stg2"],
            "model.p.stg2": [],
        },
        "nodes": {
            "model.p.stg": {"name": "stg", "resource_type": "model", "config": {"materialized": "view"}},
            "model.p.stg2": {"name": "stg2", "resource_type": "model", "config": {"materialized": "view"}},
            "model.p.mart": {"name": "mart", "resource_type": "model", "config": {"materialized": "table"}},
            "test.p.check": {"name": "check", "resource_type": "test", "config": {"materialized": "test"}},
        },
    }))
    return path


def test_a_source_feeding_a_table_through_a_view_rebuilds_it_and_changes_the_marts(tmp_path):
    plan = rebuild_state.plan_build(_manifest(tmp_path), ("read",))
    assert plan.rebuilds_anything and plan.marts_changed
    assert plan.exclude == ("stg2",)
    assert plan.untouched_models == {"stg2"}


def test_a_source_reaching_only_views_leaves_the_marts_alone(tmp_path):
    plan = rebuild_state.plan_build(_manifest(tmp_path), ("view_only",))
    assert plan.rebuilds_anything and not plan.marts_changed
    assert plan.exclude == ("check", "mart", "stg")


def test_a_raw_table_no_model_reads_has_nothing_for_dbt_to_do(tmp_path):
    plan = rebuild_state.plan_build(_manifest(tmp_path), ("unread",))
    assert not plan.rebuilds_anything and not plan.marts_changed


def test_a_test_joining_a_rebuilt_model_to_an_untouched_one_still_runs(tmp_path):
    """The source of the change reaches it through one parent, and that is enough."""
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({
        "sources": {"source.p.raw.read": {"name": "read", "source_name": "raw"}},
        "child_map": {
            "source.p.raw.read": ["model.p.fact"],
            "model.p.fact": ["test.p.joins"],
            "model.p.dim": ["test.p.joins"],
        },
        "nodes": {
            "model.p.fact": {"name": "fact", "resource_type": "model", "config": {"materialized": "table"}},
            "model.p.dim": {"name": "dim", "resource_type": "model", "config": {"materialized": "table"}},
            "test.p.joins": {"name": "joins", "resource_type": "test", "config": {"materialized": "test"}},
        },
    }))
    assert rebuild_state.plan_build(path, ("read",)).exclude == ("dim",)


@pytest.fixture
def warehouse(tmp_path):
    path = str(tmp_path / "datalake.duckdb")
    build_marts(path)
    con = duckdb.connect(path)
    rebuild_state.ensure(con)
    con.close()
    rebuild_state.record_built(path, marts_changed=True)
    return path


def test_the_serving_copy_is_left_alone_until_a_mart_changes(warehouse, tmp_path):
    target = tmp_path / "serving.duckdb"
    first, replaced = serving.publish_if_changed(warehouse, str(target))
    assert replaced
    inode = target.stat().st_ino

    again, replaced = serving.publish_if_changed(warehouse, str(target))
    assert not replaced
    assert target.stat().st_ino == inode, "the file was rewritten"
    assert (again.rows, sorted(again.tables)) == (first.rows, sorted(first.tables))

    rebuild_state.record_built(warehouse, marts_changed=True)
    _, replaced = serving.publish_if_changed(warehouse, str(target))
    assert replaced
    assert target.stat().st_ino != inode


def test_a_missing_serving_copy_is_published_whatever_the_marker_says(warehouse, tmp_path):
    target = tmp_path / "serving.duckdb"
    serving.publish_if_changed(warehouse, str(target))
    target.unlink()
    _, replaced = serving.publish_if_changed(warehouse, str(target))
    assert replaced and target.exists()


def test_a_refused_publish_is_retried_because_the_marker_did_not_move(warehouse, tmp_path, monkeypatch):
    target = tmp_path / "serving.duckdb"
    serving.publish_if_changed(warehouse, str(target))
    rebuild_state.record_built(warehouse, marts_changed=True)

    real = serving.publish
    monkeypatch.setattr(serving, "publish", lambda *_: (_ for _ in ()).throw(RuntimeError("refused")))
    with pytest.raises(RuntimeError):
        serving.publish_if_changed(warehouse, str(target))
    monkeypatch.setattr(serving, "publish", real)

    _, replaced = serving.publish_if_changed(warehouse, str(target))
    assert replaced


def test_a_warehouse_with_no_generation_always_publishes(tmp_path):
    path = str(tmp_path / "datalake.duckdb")
    build_marts(path)
    target = str(tmp_path / "serving.duckdb")
    assert serving.publish_if_changed(path, target)[1]
    assert serving.publish_if_changed(path, target)[1]
