"""The lake layout: paths, and the migration that produces them.

These are cheap tests of a thing that is expensive to get wrong. A path built by
hand somewhere else, or a migration that maps two datasets onto one prefix, loses
data quietly -- the pipeline keeps writing and the reader keeps reading, just not
the same objects.
"""

import pytest

from homelab_datalake import layout, migrate_layout


def test_every_dataset_has_a_unique_prefix():
    prefixes = [d.prefix for d in layout.ALL_DATASETS]
    assert len(prefixes) == len(set(prefixes))


def test_no_prefix_is_a_parent_of_another():
    """A prefix under another prefix makes a glob read two datasets as one.

    `raw/npm_access` and `raw/npm_access_rejects` were safe only because of the
    trailing separator. Nesting one dataset inside another would not be.
    """
    prefixes = sorted(d.prefix for d in layout.ALL_DATASETS)
    for outer in prefixes:
        for inner in prefixes:
            assert outer == inner or not inner.startswith(outer + "/")


def test_prefix_starts_with_its_layer():
    for dataset in layout.ALL_DATASETS:
        assert dataset.layer in layout.LAYERS
        assert dataset.prefix.split("/")[0] == dataset.layer


def test_marts_have_no_source_segment():
    """A mart joins sources, so naming one in its path would mislead half the time."""
    for dataset in layout.datasets_in(layout.LAYER_MARTS):
        assert dataset.source == ""
        assert "//" not in dataset.prefix
        assert len(dataset.prefix.split("/")) == 3


def test_raw_and_curated_name_their_source():
    for layer in (layout.LAYER_RAW, layout.LAYER_CURATED):
        for dataset in layout.datasets_in(layer):
            assert dataset.source, dataset.table
            assert len(dataset.prefix.split("/")) == 4


def test_unknown_table_names_itself_in_the_error():
    with pytest.raises(KeyError, match="camera_thermal"):
        layout.dataset("camera_thermal")


def test_migration_moves_every_legacy_prefix():
    moved = {old for old, _ in layout.migrations()}
    assert moved == {f"raw/{d.table}" for d in layout.RAW_DATASETS}


# --------------------------------------------------------------------------------
# single_schema: a per-dataset promise LakeSession trusts to skip union_by_name
# (homelab_datalake.api.query.view_create_statement). Wrong in either direction is
# a real failure mode -- flagged and evolving raises a schema-mismatch error mid
# publish; unflagged and fixed just costs a footer read it did not need to -- so
# both the *set* of flagged datasets and each writer's actual behaviour are
# checked here, not only the flag.
# --------------------------------------------------------------------------------


def test_only_datasets_with_a_provably_fixed_writer_schema_are_flagged():
    """Enumerated explicitly so adding one is a deliberate, reviewed decision.

    jellyfin_playback and jellyfin_activity qualify because every partition is
    built through project(..., _PLAYBACK_SCHEMA) / project(..., _ACTIVITY_SCHEMA)
    in assets/jellyfin.py -- Python code with one hardcoded pyarrow schema, not a
    dbt model or a registry that gains columns. See the next test for the writer
    side of this claim.
    """
    flagged = {d.table for d in layout.ALL_DATASETS if d.single_schema}
    assert flagged == {"jellyfin_playback", "jellyfin_activity"}


@pytest.mark.parametrize("function_name, schema_name", [
    # jellyfin_playback_history_raw (the backfill asset) does not build the
    # Arrow table itself -- it delegates each hour to this helper, "written
    # exactly the way jellyfin_playback_raw writes a partition" per its own
    # docstring -- so this is the function whose schema argument actually
    # matters, not the asset that calls it.
    ("jellyfin_playback_raw", "_PLAYBACK_SCHEMA"),
    ("_write_playback_history_hour", "_PLAYBACK_SCHEMA"),
    ("jellyfin_activity_raw", "_ACTIVITY_SCHEMA"),
    ("_write_activity_history_day", "_ACTIVITY_SCHEMA"),
])
def test_every_writer_of_a_flagged_dataset_still_uses_the_declared_schema(
    function_name, schema_name
):
    """The regression this guards against: single_schema=True is a promise that
    LakeSession relies on without checking it itself (checking it would mean
    reading every footer, which is the cost the flag exists to avoid). If a
    writer changed to project onto a different schema, or none, a flagged
    dataset could grow a file union_by_name would have caught and single_schema
    silently will not.

    Every function that actually builds the Arrow table for a jellyfin_playback
    or jellyfin_activity partition -- the two per-partition assets, and the two
    helpers their backfill counterparts delegate to -- is inspected for the
    schema object it hands `pa.Table.from_pylist`: not run, just read, so this
    needs no MinIO and no Dagster context. Must still be the one shared
    module-level constant. A schema built inline, or a second constant that
    happens to look the same, would not be caught by this same-name check and
    is exactly why the datasets above are also cross-checked against the *set*
    of flagged tables instead of trusted alone.
    """
    import ast
    import inspect

    from homelab_datalake.assets import jellyfin as jellyfin_assets

    # Not inspect.getsource(getattr(module, name)) -- these are @asset/@multi_asset
    # functions, so the module attribute is a Dagster AssetsDefinition wrapping the
    # original function, not the function itself, and inspect cannot find source
    # for that. Parsing the module's own source and pulling out the one FunctionDef
    # sidesteps the decorator entirely.
    module_source = inspect.getsource(jellyfin_assets)
    tree = ast.parse(module_source)
    node = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.FunctionDef) and n.name == function_name),
        None,
    )
    assert node is not None, f"no function named {function_name} in {jellyfin_assets.__name__}"
    source = ast.get_source_segment(module_source, node)
    assert f"schema={schema_name}" in source, (
        f"{function_name} no longer writes with {schema_name}; either restore it "
        "or set single_schema=False on the corresponding dataset in layout.py, "
        "because LakeSession trusts the flag without checking it at bind time."
    )


def test_plan_keeps_the_partition_path():
    key = "raw/npm_access/host=fraserv/dt=2026-08-27/data.parquet"
    plan = migrate_layout.plan({key: 100})
    assert len(plan.moves) == 1
    assert plan.moves[0].new_key == (
        "raw/infrastructure/npm/access/host=fraserv/dt=2026-08-27/data.parquet"
    )


def test_plan_does_not_confuse_a_prefix_with_its_neighbour():
    """`raw/npm_access` is a string prefix of `raw/npm_access_rejects`."""
    plan = migrate_layout.plan({
        "raw/npm_access/host=amb/dt=2026-08-01/data.parquet": 1,
        "raw/npm_access_rejects/host=amb/dt=2026-08-01/data.parquet": 1,
    })
    landed = {m.new_key for m in plan.moves}
    assert landed == {
        "raw/infrastructure/npm/access/host=amb/dt=2026-08-01/data.parquet",
        "raw/infrastructure/npm/access_rejects/host=amb/dt=2026-08-01/data.parquet",
    }


def test_plan_is_idempotent():
    """Re-running after a successful migration must move nothing."""
    already = {"raw/infrastructure/npm/access/dt=2026-08-01/data.parquet": 1}
    plan = migrate_layout.plan(already)
    assert plan.moves == []
    assert plan.already_moved == 1
    assert plan.unknown == []


def test_unrecognised_objects_are_reported_not_moved():
    plan = migrate_layout.plan({"scratch/notes.txt": 1})
    assert plan.moves == []
    assert plan.unknown == ["scratch/notes.txt"]


def test_apply_moves_files_and_leaves_nothing_behind(tmp_path):
    from homelab_datalake.resources import LakeStorage

    lake = LakeStorage(local_root=str(tmp_path))
    old = tmp_path / "raw/camera_events/dt=2026-08-01"
    old.mkdir(parents=True)
    (old / "data.parquet").write_bytes(b"payload")

    plan = migrate_layout.plan(migrate_layout.inventory(lake))
    assert migrate_layout.apply(lake, plan) == 1

    moved = tmp_path / "raw/monitoring/camera/events/dt=2026-08-01/data.parquet"
    assert moved.read_bytes() == b"payload"
    assert not (tmp_path / "raw/camera_events").exists()
    assert migrate_layout.plan(migrate_layout.inventory(lake)).moves == []
