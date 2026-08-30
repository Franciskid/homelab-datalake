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
    with pytest.raises(KeyError, match="camera_zones"):
        layout.dataset("camera_zones")


def test_migration_moves_every_legacy_prefix():
    moved = {old for old, _ in layout.migrations()}
    assert moved == {f"raw/{d.table}" for d in layout.RAW_DATASETS}


def test_plan_keeps_the_partition_path():
    key = "raw/npm_access/host=primary/dt=2026-08-27/data.parquet"
    plan = migrate_layout.plan({key: 100})
    assert len(plan.moves) == 1
    assert plan.moves[0].new_key == (
        "raw/infrastructure/npm/access/host=primary/dt=2026-08-27/data.parquet"
    )


def test_plan_does_not_confuse_a_prefix_with_its_neighbour():
    """`raw/npm_access` is a string prefix of `raw/npm_access_rejects`."""
    plan = migrate_layout.plan({
        "raw/npm_access/host=secondary/dt=2026-08-01/data.parquet": 1,
        "raw/npm_access_rejects/host=secondary/dt=2026-08-01/data.parquet": 1,
    })
    landed = {m.new_key for m in plan.moves}
    assert landed == {
        "raw/infrastructure/npm/access/host=secondary/dt=2026-08-01/data.parquet",
        "raw/infrastructure/npm/access_rejects/host=secondary/dt=2026-08-01/data.parquet",
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
