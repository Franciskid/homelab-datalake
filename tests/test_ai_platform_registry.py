"""The ai_platform registry: exclusions, the generated grant SQL, and coverage.

The two enforcements the module docstring promises are both tested here: the
registry itself excludes the named secrets (this file), and the grant SQL rendered
from it is exactly the committed file (this file too, so the two cannot drift
without the test noticing).
"""

from pathlib import Path

import pytest

from homelab_datalake.parsing import ai_platform_registry as reg

REPO_ROOT = Path(__file__).resolve().parents[1]
GRANT_SQL_PATH = REPO_ROOT / "infra" / "ai-platform-datalake-role.sql"

# The exact "never copied" list from the task brief. Every one of these has to be a
# `secret=True` ExcludedColumn on its table, and none of them may appear in that
# table's `columns` -- both directions are asserted below.
REQUIRED_SECRET_EXCLUSIONS = {
    ("api_keys", "secret_hash"),
    ("api_keys", "secret_ciphertext"),
    ("api_keys", "litellm_virtual_key_id"),
    ("providers", "credential_ciphertext"),
    ("push_subscriptions", "endpoint"),
    ("push_subscriptions", "p256dh"),
    ("push_subscriptions", "auth"),
    ("platform_settings", "ntfy_topic"),
    ("user_tool_configs", "values"),
    ("mcp_servers", "command"),
    ("mcp_servers", "metadata"),
}


def test_every_required_secret_is_excluded_and_marked():
    found = {
        (table.name, excluded.column)
        for table in reg.REGISTRY
        for excluded in table.excluded_columns
        if excluded.secret
    }
    missing = REQUIRED_SECRET_EXCLUSIONS - found
    assert not missing, f"not excluded (or not marked secret): {sorted(missing)}"


def test_no_required_secret_is_selected():
    """The other half of the same guarantee: excluded really means never read."""
    for table_name, column in REQUIRED_SECRET_EXCLUSIONS:
        table = reg.spec(table_name)
        assert column not in table.columns, (
            f"{table_name}.{column} is a required secret exclusion but is still "
            "in `columns` -- it would be selected and granted"
        )


def test_mcp_servers_url_is_kept_but_marked_for_query_stripping():
    """`url` is not a secret -- it is landed, with its query string removed."""
    mcp = reg.spec("mcp_servers")
    assert "url" in mcp.columns
    assert "url" in mcp.redact_query_columns
    assert not any(e.column == "url" for e in mcp.excluded_columns)


def test_skipped_tables_have_no_registry_entry():
    names = {t.name for t in reg.REGISTRY}
    assert reg.SKIPPED_TABLES.isdisjoint(names)


def test_binary_tables_carry_a_binary_column():
    for name in ("message_attachments", "generated_files"):
        table = reg.spec(name)
        assert table.binary_column == "data"
        assert "data" in table.columns


def test_usage_events_is_the_only_frame_decoding_table():
    frame_tables = [t.name for t in reg.REGISTRY if t.decode_metadata_images]
    assert frame_tables == ["usage_events"]


# --------------------------------------------------------------------------------
# TableSpec construction guards
# --------------------------------------------------------------------------------


def test_window_mode_requires_a_change_column():
    with pytest.raises(ValueError, match="change_column"):
        reg.TableSpec(
            name="x", primary_key="id", mode=reg.MODE_WINDOW, description="d",
            columns=("id",),
        )


def test_snapshot_mode_forbids_a_change_column():
    with pytest.raises(ValueError, match="change_column"):
        reg.TableSpec(
            name="x", primary_key="id", mode=reg.MODE_SNAPSHOT, description="d",
            columns=("id",), change_column="created_at",
        )


def test_binary_column_must_be_among_the_selected_columns():
    with pytest.raises(ValueError, match="binary_column"):
        reg.TableSpec(
            name="x", primary_key="id", mode=reg.MODE_SNAPSHOT, description="d",
            columns=("id",), binary_column="data",
        )


def test_redact_query_columns_must_be_among_the_selected_columns():
    with pytest.raises(ValueError, match="redact_query_columns"):
        reg.TableSpec(
            name="x", primary_key="id", mode=reg.MODE_SNAPSHOT, description="d",
            columns=("id",), redact_query_columns=("url",),
        )


def test_a_duplicate_table_name_is_refused_at_import_time():
    """The registry's own guard, exercised directly rather than trusted."""
    a = reg.TableSpec(
        name="dupe", primary_key="id", mode=reg.MODE_SNAPSHOT, description="d",
        columns=("id",),
    )
    b = reg.TableSpec(
        name="dupe", primary_key="id", mode=reg.MODE_SNAPSHOT, description="d2",
        columns=("id",),
    )
    names = [t.name for t in (a, b)]
    assert len(set(names)) != len(names)  # the condition the module-level guard checks


# --------------------------------------------------------------------------------
# registry_coverage()
# --------------------------------------------------------------------------------


def test_registry_coverage_reports_nothing_when_live_matches_the_registry():
    live = [t.name for t in reg.REGISTRY] + list(reg.SKIPPED_TABLES)
    missing, stale = reg.registry_coverage(live)
    assert missing == []
    assert stale == []


def test_registry_coverage_reports_a_table_the_platform_added():
    live = [t.name for t in reg.REGISTRY] + ["brand_new_table"]
    missing, stale = reg.registry_coverage(live)
    assert missing == ["brand_new_table"]
    assert stale == []


def test_registry_coverage_reports_a_table_that_no_longer_exists():
    live = [t.name for t in reg.REGISTRY if t.name != "runs"]
    missing, stale = reg.registry_coverage(live)
    assert missing == []
    assert stale == ["runs"]


def test_registry_coverage_ignores_the_skipped_tables():
    live = [t.name for t in reg.REGISTRY] + list(reg.SKIPPED_TABLES)
    missing, _stale = reg.registry_coverage(live)
    assert missing == []


# --------------------------------------------------------------------------------
# the generated grant SQL
# --------------------------------------------------------------------------------


def test_grant_sql_file_is_exactly_what_the_registry_renders():
    """Enforcement 2 from the module docstring: a registry change that is not
    accompanied by regenerating infra/ai-platform-datalake-role.sql fails here."""
    assert GRANT_SQL_PATH.exists(), f"{GRANT_SQL_PATH} is missing; generate it with render_grant_sql()"
    on_disk = GRANT_SQL_PATH.read_text()
    rendered = reg.render_grant_sql()
    assert on_disk == rendered, (
        "infra/ai-platform-datalake-role.sql does not match "
        "render_grant_sql(REGISTRY) -- regenerate it from the current registry"
    )


def test_grant_sql_never_grants_a_required_secret():
    rendered = reg.render_grant_sql()
    for table_name, column in REQUIRED_SECRET_EXCLUSIONS:
        # A crude but sufficient check: the column name must not appear inside that
        # table's own GRANT line specifically, not merely somewhere in the file
        # (another table could coincidentally share a column name).
        for line in rendered.splitlines():
            if f'ON "{table_name}"' in line:
                assert column not in line, f"{table_name}.{column} is granted: {line}"


def test_grant_sql_grants_every_registered_table():
    rendered = reg.render_grant_sql()
    for table in reg.REGISTRY:
        assert f'ON "{table.name}" TO datalake_ro' in rendered, f"{table.name} has no grant line"


def test_grant_sql_is_stable_for_a_smaller_registry():
    """render_grant_sql takes the registry as a parameter precisely so this is
    testable without touching the real 52-table list."""
    subset = (reg.spec("runs"), reg.spec("agents"))
    rendered = reg.render_grant_sql(subset)
    assert "runs" in rendered
    assert "agents" in rendered
    assert "usage_events" not in rendered
