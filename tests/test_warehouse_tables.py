"""The warehouse's source list against the dataset registry.

`assets/warehouse.py` keeps its own `_SOURCES` tuple -- it needs the Arrow schema and
the upstream asset, which `layout` does not carry -- and that second list is exactly
the kind of thing that drifts. It did: five datasets were registered in `layout`,
landed in the lake by their assets, and then had no table in the warehouse, so every
dbt model reading them failed with "Table with name camera_zones does not exist".

The symptom was loud, which is luck rather than design. A dataset *removed* from
`layout` and left in `_SOURCES` would fail the same way, and one added to `_SOURCES`
without a prefix in `layout` cannot even be built. So both directions are asserted.
"""

import logging
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from homelab_datalake import layout
from homelab_datalake.assets.warehouse import _LATEST_BY, _SOURCES, empty_table_ddl, load_table
from homelab_datalake.resources import LakeStorage


def test_every_registered_raw_dataset_has_a_warehouse_table():
    registered = {d.table for d in layout.RAW_DATASETS if not d.lake_only}
    loaded = {name for name, *_ in _SOURCES}
    missing = registered - loaded
    assert not missing, (
        f"{sorted(missing)} are registered in layout.RAW_DATASETS and land in the lake, "
        "but assets/warehouse.py will not create a table for them -- every dbt model "
        "reading them fails with a catalog error"
    )


def test_no_warehouse_table_is_built_from_an_unregistered_prefix():
    registered = {d.table for d in layout.RAW_DATASETS if not d.lake_only}
    loaded = {name for name, *_ in _SOURCES}
    assert not (loaded - registered), (
        f"{sorted(loaded - registered)} are loaded into the warehouse but not registered "
        "in layout, so nothing decides where they live in the lake"
    )


def test_a_lake_only_dataset_is_never_loaded_into_the_warehouse():
    """The flag is the reason a dataset is absent, so it must actually keep it out.

    Prometheus samples are ~60 M rows a day. Loaded by mistake, they would multiply the
    warehouse file within a week, and analytics_hourly rebuilds that file in full every hour.
    """
    lake_only = {d.table for d in layout.RAW_DATASETS if d.lake_only}
    loaded = {name for name, *_ in _SOURCES}
    assert lake_only, "expected the Prometheus samples to be lake-only"
    assert not (lake_only & loaded), f"{sorted(lake_only & loaded)} are lake-only but loaded"


def test_the_declared_prefix_is_the_one_the_layout_registry_gives():
    for name, prefix, *_ in _SOURCES:
        assert prefix == layout.prefix(name), (
            f"{name} is loaded from {prefix} but layout puts it at {layout.prefix(name)}"
        )


def test_a_source_with_nothing_landed_still_gets_its_real_columns():
    """Nested types included -- the fallback must not be the thing that breaks a build."""
    for name, _prefix, schema, *_ in _SOURCES:
        ddl = empty_table_ddl(f"raw.{name}", schema)
        for field in schema:
            assert f'"{field.name}"' in ddl, f"{name}.{field.name} missing from the DDL"


def test_struct_and_list_columns_render_as_duckdb_types():
    schema = pa.schema([
        ("counts", pa.struct([("person", pa.int64()), ("vehicle", pa.int64())])),
        ("tags", pa.list_(pa.string())),
    ])
    ddl = empty_table_ddl("raw.example", schema)
    assert 'STRUCT("person" BIGINT, "vehicle" BIGINT)' in ddl
    assert "VARCHAR[]" in ddl


_LOG = logging.getLogger(__name__)
_SCHEMA = pa.schema([("dt", pa.string()), ("id", pa.string()), ("updated_at", pa.string())])


def _land(lake: LakeStorage, prefix: str, day: str, rows: list[tuple[str, str]]) -> None:
    path = Path(lake.local_root) / prefix / f"dt={day}" / "data.parquet"
    path.parent.mkdir(parents=True)
    table = {"dt": [day] * len(rows), "id": [r[0] for r in rows], "updated_at": [r[1] for r in rows]}
    pq.write_table(pa.table(table, schema=_SCHEMA), path)


@pytest.fixture
def warehouse_con():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw;")
    yield con
    con.close()


def test_a_row_touched_after_the_window_keeps_only_its_newest_copy(tmp_path, warehouse_con):
    """Conversation eca152f9 landed on 9 Oct and again on 28 Oct, and froze the marts for days."""
    lake = LakeStorage(local_root=str(tmp_path))
    _land(lake, "conv", "2026-10-09", [("a", "2026-10-09"), ("b", "2026-10-09")])
    _land(lake, "conv", "2026-10-28", [("a", "2026-10-28")])

    load_table(warehouse_con, lake, "platform_conversations", "conv", _SCHEMA, _LOG)

    rows = warehouse_con.execute(
        "SELECT id, updated_at FROM raw.platform_conversations ORDER BY id"
    ).fetchall()
    assert rows == [("a", "2026-10-28"), ("b", "2026-10-09")]


def test_every_table_windowed_on_a_moving_column_is_reduced_to_its_newest_copy():
    moving = {"updated_at", "last_seen_at", "indexed_at"}
    reduced = {table for table, (_key, change) in _LATEST_BY.items() if change in moving}
    assert {
        "platform_approvals", "platform_conversations", "platform_document_chunks",
        "platform_incidents", "platform_media_items", "platform_resource_mappings",
    } <= reduced
    assert all(isinstance(key, str) for key, _change in _LATEST_BY.values())


def test_a_table_nothing_has_landed_for_is_created_empty(tmp_path, warehouse_con):
    lake = LakeStorage(local_root=str(tmp_path))
    load_table(warehouse_con, lake, "example", "nothing/here", _SCHEMA, _LOG)
    assert warehouse_con.execute("SELECT count(*) FROM raw.example").fetchone() == (0,)


class _UnreachableS3:
    """What DuckDB does when the bucket does not answer: the same error as no files."""

    def execute(self, statement):
        if "read_parquet" in statement:
            raise duckdb.IOException("Could not establish connection")
        raise AssertionError(f"a table was created anyway: {statement}")


def test_a_lake_that_cannot_be_read_fails_the_load_rather_than_emptying_the_table(tmp_path):
    """The 25 Oct 2026 outage: every raw table went empty and dbt passed on them."""
    lake = LakeStorage(local_root=str(tmp_path))
    _land(lake, "conv", "2026-10-09", [("a", "2026-10-09")])
    with pytest.raises(duckdb.IOException):
        load_table(_UnreachableS3(), lake, "example", "conv", _SCHEMA, _LOG)


def test_a_bucket_that_does_not_answer_the_listing_fails_the_load_too(tmp_path, monkeypatch):
    def refuse(_self, _prefix):
        raise ConnectionError("Temporary failure in name resolution")

    monkeypatch.setattr(LakeStorage, "list_keys", refuse)
    lake = LakeStorage(local_root=str(tmp_path))
    with pytest.raises(ConnectionError):
        load_table(_UnreachableS3(), lake, "example", "conv", _SCHEMA, _LOG)


def test_tmdb_reports_a_read_failure_instead_of_treating_it_as_nothing_enriched(
    tmp_path, monkeypatch
):
    from homelab_datalake.assets import tmdb

    class Unreachable:
        def configure(self):
            return None

        def execute(self, _statement):
            raise duckdb.IOException("Could not establish connection")

        def close(self):
            return None

    monkeypatch.setattr(tmdb.duckdb, "connect", lambda *a, **k: Unreachable())
    monkeypatch.setattr(LakeStorage, "configure_duckdb", lambda _self, _con: None)
    lake = LakeStorage(local_root=str(tmp_path))
    assert tmdb._existing(lake) == {}, "nothing landed is still a normal first run"

    landed = Path(lake.local_root) / tmdb.TMDB_PREFIX / "data.parquet"
    landed.parent.mkdir(parents=True)
    landed.write_bytes(b"landed")
    with pytest.raises(duckdb.IOException):
        tmdb._existing(lake)
