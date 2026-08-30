"""Publishing the derived layers back into the lake.

The property that matters is that a republish *replaces*. A layer that accumulated
would keep serving a partition that dbt no longer produces, and nothing would say so
-- the object is still there and still readable, it is just no longer true.
"""

from pathlib import Path

import duckdb
import pytest

from homelab_datalake import layout
from homelab_datalake.assets import publish
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def warehouse(tmp_path):
    path = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone='UTC';")
    con.execute("CREATE SCHEMA main_marts;")
    con.execute("""
        CREATE TABLE main_marts.agg_traffic_daily AS
        SELECT (DATE '2026-08-01' + (i % 3)::INTEGER) AS partition_date, i AS requests
        FROM range(0, 6) t(i)
    """)
    con.execute("CREATE SCHEMA main_staging;")
    con.execute("CREATE VIEW main_staging.stg_tmdb_titles AS SELECT 1 AS tmdb_id")
    yield con
    con.close()


@pytest.fixture
def lake(tmp_path):
    return LakeStorage(local_root=str(tmp_path / "lake"))


def test_a_partitioned_dataset_gets_one_object_per_value(warehouse, lake):
    result = publish.publish_dataset(warehouse, lake, layout.dataset("agg_traffic_daily"))
    assert result.objects == 3
    assert result.rows == 6
    root = Path(lake.local_root)
    assert sorted(p.parent.name for p in root.rglob("*.parquet")) == [
        "partition_date=2026-08-01",
        "partition_date=2026-08-02",
        "partition_date=2026-08-03",
    ]


def test_an_unpartitioned_dataset_gets_one_object(warehouse, lake):
    result = publish.publish_dataset(warehouse, lake, layout.dataset("stg_tmdb_titles"))
    assert result.objects == 1
    assert (Path(lake.local_root) / "curated/media/tmdb/titles/data.parquet").exists()


def test_a_partition_that_no_longer_exists_is_removed(warehouse, lake):
    dataset = layout.dataset("agg_traffic_daily")
    publish.publish_dataset(warehouse, lake, dataset)

    warehouse.execute(
        "DELETE FROM main_marts.agg_traffic_daily WHERE partition_date = DATE '2026-08-03'"
    )
    result = publish.publish_dataset(warehouse, lake, dataset)

    assert result.objects == 2
    assert result.removed == 1
    root = Path(lake.local_root)
    assert not (root / dataset.prefix / "partition_date=2026-08-03").exists()


def test_republishing_the_same_data_removes_nothing(warehouse, lake):
    dataset = layout.dataset("agg_traffic_daily")
    publish.publish_dataset(warehouse, lake, dataset)
    assert publish.publish_dataset(warehouse, lake, dataset).removed == 0


def test_published_objects_are_readable_as_one_dataset(warehouse, lake):
    """The partitions must glob back together, or the layout is decoration."""
    publish.publish_dataset(warehouse, lake, layout.dataset("agg_traffic_daily"))
    target = lake.duckdb_scan_target(layout.prefix("agg_traffic_daily"))
    rows = duckdb.connect(":memory:").execute(
        f"SELECT count(*), count(DISTINCT partition_date) FROM read_parquet('{target}')"  # noqa: S608
    ).fetchone()
    assert rows == (6, 3)


def test_a_missing_model_says_which_one(warehouse, lake):
    with pytest.raises(RuntimeError, match="fct_http_request"):
        publish.publish_dataset(warehouse, lake, layout.dataset("fct_http_request"))


def test_lineage_points_at_the_model_that_produced_it():
    """Each published object depends on its own dbt model, not on the whole build."""
    assert publish.dbt_asset_key("stg_npm_access").path == ["staging", "stg_npm_access"]
    assert publish.dbt_asset_key("int_npm_requests").path == ["intermediate", "int_npm_requests"]
    assert publish.dbt_asset_key("fct_http_request").path == ["marts", "fct_http_request"]


def test_every_registered_dataset_has_a_dbt_model_upstream():
    for dataset in layout.datasets_in(layout.LAYER_CURATED) + layout.datasets_in(layout.LAYER_MARTS):
        key = publish.dbt_asset_key(dataset.table)
        assert key.path[0] in ("staging", "intermediate", "marts")


def test_publishing_does_not_take_the_write_lock(marts_db, lake, tmp_path):
    """The three publish steps run at once and all read the same DuckDB file.

    The first production run of this failed exactly here: two steps died with
    "Conflicting lock is held" while the third did the work. DuckDB permits one
    writer, so nothing that only reads may open the file read-write.

    Built on the shared marts fixture rather than on this module's small one,
    because publishing the serving copy also validates the semantic layer, and that
    needs every mart the tools name.
    """
    from homelab_datalake import serving

    holder = duckdb.connect(marts_db, read_only=True)
    reader = duckdb.connect(":memory:")
    try:
        reader.execute(f"ATTACH '{marts_db}' AS warehouse (READ_ONLY);")
        reader.execute("USE warehouse;")
        assert reader.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone() == (200,)

        # And the serving copy publishes while both of those handles are still open.
        published = serving.publish(marts_db, str(tmp_path / "serving.duckdb"))
        assert "fct_llm_call" in published.tables
    finally:
        reader.close()
        holder.close()
