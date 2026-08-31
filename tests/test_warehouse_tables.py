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

import pyarrow as pa

from homelab_datalake import layout
from homelab_datalake.assets.warehouse import _SOURCES, empty_table_ddl


def test_every_registered_raw_dataset_has_a_warehouse_table():
    registered = {d.table for d in layout.RAW_DATASETS}
    loaded = {name for name, *_ in _SOURCES}
    missing = registered - loaded
    assert not missing, (
        f"{sorted(missing)} are registered in layout.RAW_DATASETS and land in the lake, "
        "but assets/warehouse.py will not create a table for them -- every dbt model "
        "reading them fails with a catalog error"
    )


def test_no_warehouse_table_is_built_from_an_unregistered_prefix():
    registered = {d.table for d in layout.RAW_DATASETS}
    loaded = {name for name, *_ in _SOURCES}
    assert not (loaded - registered), (
        f"{sorted(loaded - registered)} are loaded into the warehouse but not registered "
        "in layout, so nothing decides where they live in the lake"
    )


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
