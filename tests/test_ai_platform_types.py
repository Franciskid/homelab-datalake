"""The Postgres -> Arrow type mapping, and the schema built from it.

PG_TYPE_OVERRIDES is committed source, read off the live schema on 2026-09-14 --
these tests check its internal consistency with the registry (every override names
a real table and a really-selected column) rather than against Postgres itself,
which no test in this suite has access to.
"""

import pyarrow as pa

from homelab_datalake.parsing.ai_platform_registry import REGISTRY, spec
from homelab_datalake.parsing.ai_platform_types import (
    PG_TYPE_OVERRIDES,
    arrow_schema_for,
    arrow_type_for,
)


def test_every_override_names_a_real_registered_table():
    registered = {t.name for t in REGISTRY}
    unknown = set(PG_TYPE_OVERRIDES) - registered
    assert not unknown, f"overrides name tables not in the registry: {unknown}"


def test_every_overridden_column_is_actually_selected():
    for table_name, columns in PG_TYPE_OVERRIDES.items():
        table = spec(table_name)
        for column in columns:
            assert column in table.columns, f"{table_name}.{column} is overridden but not selected"


def test_no_override_names_the_binary_column():
    """The bytea column never reaches the row schema at all -- see arrow_schema_for
    -- so giving it a type override would be dead configuration."""
    for table_name, columns in PG_TYPE_OVERRIDES.items():
        table = spec(table_name)
        if table.binary_column:
            assert table.binary_column not in columns


def test_default_type_is_string():
    assert arrow_type_for("runs", "a_column_with_no_override") == pa.string()


def test_an_overridden_column_gets_its_mapped_type():
    assert arrow_type_for("runs", "cost_eur") == pa.float64()
    assert arrow_type_for("runs", "input_tokens") == pa.int64()
    assert arrow_type_for("runs", "started_at") == pa.timestamp("us", tz="UTC")


def test_a_table_with_no_overrides_at_all_still_resolves():
    # audit_events has exactly one override (occurred_at); every other column on it
    # should fall through to the string default without raising.
    assert arrow_type_for("audit_events", "action") == pa.string()


def test_arrow_schema_for_starts_with_dt_and_covers_every_selected_column():
    for table in REGISTRY:
        schema = arrow_schema_for(table)
        assert schema.names[0] == "dt"
        names = set(schema.names)
        for column in table.columns:
            if column == table.binary_column:
                assert column not in names
            else:
                assert column in names, f"{table.name}.{column} missing from its Arrow schema"


def test_binary_tables_get_object_key_instead_of_the_bytea_column():
    for table_name in ("message_attachments", "generated_files"):
        schema = arrow_schema_for(spec(table_name))
        assert "data" not in schema.names
        assert "object_key" in schema.names
        assert schema.field("object_key").type == pa.string()


def test_usage_events_gets_the_frame_columns():
    schema = arrow_schema_for(spec("usage_events"))
    assert schema.field("frame_keys").type == pa.list_(pa.string())
    assert schema.field("frame_count").type == pa.int64()
    assert schema.field("frame_bytes").type == pa.int64()


def test_only_usage_events_gets_the_frame_columns():
    for table in REGISTRY:
        if table.name == "usage_events":
            continue
        schema = arrow_schema_for(table)
        assert "frame_keys" not in schema.names
        assert "frame_count" not in schema.names
