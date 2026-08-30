"""The second copy of raw/, and the restore that makes it a backup rather than a copy.

The point of these tests is the restore. A mirror that copies is easy to write and
easy to believe in; the question that matters is whether the objects come back, and
that question is only answered by putting them back and re-listing. So the restore is
exercised here against a lake that has actually lost data, not asserted from the
return value of the function that claims to have restored it.

Everything runs on the local backend, which is the same code path as MinIO for the
decisions -- what to copy, what to skip, what to put back -- and differs only in the
call that moves the bytes.
"""

import pathlib
import shutil
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pytest

from homelab_datalake import layout
from homelab_datalake.assets import backup
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def lake(tmp_path) -> LakeStorage:
    return LakeStorage(local_root=str(tmp_path / "lake"))


@pytest.fixture
def vault(lake) -> LakeStorage:
    return backup.backup_storage(lake)


def _land(lake: LakeStorage, table: str, day: str, rows: int = 1) -> str:
    dataset = layout.dataset(table)
    key = f"{dataset.prefix}/{dataset.partition_column}={day}/data.parquet"
    lake.write_table(key, pa.table({"n": pa.array(list(range(rows)))}))
    return key


# --------------------------------------------------------------------------------
# what is copied, and what is not
# --------------------------------------------------------------------------------


def test_only_raw_is_copied(lake, vault):
    """curated/ and marts/ are rebuilt from raw/ by analytics_daily.

    Copying a derivation costs storage and buys nothing a re-run would not.
    """
    _land(lake, "npm_access", "2026-08-01")
    lake.write_table("marts/infrastructure/agg_traffic_daily/partition_date=2026-08-01/d.parquet",
                     pa.table({"n": pa.array([1])}))
    lake.write_table("curated/infrastructure/npm/requests/partition_date=2026-08-01/d.parquet",
                     pa.table({"n": pa.array([1])}))

    result = backup.mirror(lake, vault)
    assert result.copied == 1
    assert list(backup.inventory(vault, "marts")) == []
    assert list(backup.inventory(vault, "curated")) == []


def test_a_second_run_copies_nothing(lake, vault):
    """Lake objects are immutable once written, so a key that matches is the object."""
    _land(lake, "npm_access", "2026-08-01")
    _land(lake, "camera_events", "2026-08-01")
    assert backup.mirror(lake, vault).copied == 2

    again = backup.mirror(lake, vault)
    assert (again.copied, again.skipped) == (0, 2)


def test_a_replaced_partition_is_copied_again(lake, vault):
    """A re-run of a partition replaces the object. The backup has to follow it."""
    _land(lake, "npm_access", "2026-08-01", rows=1)
    backup.mirror(lake, vault)

    _land(lake, "npm_access", "2026-08-01", rows=500)
    result = backup.mirror(lake, vault)
    assert result.copied == 1

    key = next(iter(backup.inventory(vault, "raw")))
    assert backup.inventory(vault, "raw")[key].size == backup.inventory(lake, "raw")[key].size


def test_the_backup_bucket_is_created_if_it_is_not_there(lake, vault):
    _land(lake, "npm_access", "2026-08-01")
    assert backup.inventory(vault, "raw") == {}
    backup.mirror(lake, vault)
    assert len(backup.inventory(vault, "raw")) == 1


# --------------------------------------------------------------------------------
# what happens to an object that leaves raw/
# --------------------------------------------------------------------------------


def test_an_object_deleted_from_the_lake_stays_in_the_backup(lake, vault):
    """This is the case the whole module exists for. If the mirror followed the
    delete it would be a copy of the lake, not a backup of it."""
    key = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)

    pathlib.Path(lake.local_root, key).unlink()

    result = backup.mirror(lake, vault)
    assert result.orphans == [key]
    assert result.expired == 0
    assert key in backup.inventory(vault, "raw")


def test_an_orphan_is_removed_only_after_the_retention_window(lake, vault):
    key = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, key).unlink()

    inside = backup.mirror(lake, vault, orphan_days=90,
                           now=datetime.now(UTC) + timedelta(days=89))
    assert inside.expired == 0
    assert key in backup.inventory(vault, "raw")

    outside = backup.mirror(lake, vault, orphan_days=90,
                            now=datetime.now(UTC) + timedelta(days=91))
    assert outside.expired == 1
    assert key not in backup.inventory(vault, "raw")


def test_retention_can_be_switched_off(lake, vault):
    """A deletion nobody noticed for a year is still a deletion worth undoing."""
    key = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, key).unlink()

    result = backup.mirror(lake, vault, orphan_days=0,
                           now=datetime.now(UTC) + timedelta(days=3650))
    assert result.expired == 0
    assert key in backup.inventory(vault, "raw")


# --------------------------------------------------------------------------------
# the scope of a restore
# --------------------------------------------------------------------------------


def test_the_scope_of_a_restore_comes_from_the_registry():
    assert backup.restore_scope() == "raw"
    assert backup.restore_scope("npm_access") == "raw/infrastructure/npm/access"
    assert backup.restore_scope("npm_access", "2026-08-01") == (
        "raw/infrastructure/npm/access/dt=2026-08-01"
    )


def test_a_derived_dataset_cannot_be_restored_because_it_is_not_backed_up():
    """Silently restoring nothing would look exactly like a successful restore."""
    with pytest.raises(ValueError, match="rebuilt from raw"):
        backup.restore_scope("agg_traffic_daily")


def test_an_unpartitioned_dataset_refuses_a_day():
    with pytest.raises(ValueError, match="not partitioned"):
        backup.restore_scope("tmdb_titles", "2026-08-01")


def test_an_unknown_dataset_names_itself():
    with pytest.raises(KeyError, match="npm_acess"):
        backup.restore_scope("npm_acess")


# --------------------------------------------------------------------------------
# the restore itself
# --------------------------------------------------------------------------------


def test_a_deleted_partition_comes_back(lake, vault):
    kept = _land(lake, "npm_access", "2026-08-01")
    lost = _land(lake, "npm_access", "2026-08-02", rows=17)
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, lost).unlink()

    plan = backup.plan_restore(lake, vault, backup.restore_scope("npm_access"))
    assert [o.key for o in plan.restore] == [lost]
    assert plan.identical == [kept]
    assert plan.differing == []

    assert backup.apply_restore(lake, plan, vault) == 1
    assert backup.verify_restore(lake, plan, vault, backup.restore_scope("npm_access")) == []
    # Re-read rather than trusting the return value: the rows have to be there.
    path = pathlib.Path(lake.local_root, lost)
    assert duckdb.connect(":memory:").execute(
        "SELECT count(*) FROM read_parquet(?)", [str(path)]
    ).fetchone()[0] == 17


def test_a_plan_changes_nothing_on_its_own(lake, vault):
    lost = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, lost).unlink()

    backup.plan_restore(lake, vault)
    assert not pathlib.Path(lake.local_root, lost).exists()


def test_a_restore_of_one_day_leaves_the_others_alone(lake, vault):
    _land(lake, "npm_access", "2026-08-01")
    second = _land(lake, "npm_access", "2026-08-02")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, second).unlink()

    scope = backup.restore_scope("npm_access", "2026-08-01")
    plan = backup.plan_restore(lake, vault, scope)
    assert plan.restore == []
    backup.apply_restore(lake, plan, vault)
    assert not pathlib.Path(lake.local_root, second).exists()


def test_a_differing_object_is_reported_before_it_is_overwritten(lake, vault):
    """Restoring over a live object is the point, and the line to read twice."""
    key = _land(lake, "npm_access", "2026-08-01", rows=1)
    backup.mirror(lake, vault)
    _land(lake, "npm_access", "2026-08-01", rows=900)

    plan = backup.plan_restore(lake, vault)
    assert plan.differing == [key]
    assert [o.key for o in plan.restore] == [key]


def test_an_object_the_backup_never_saw_is_named_not_deleted(lake, vault):
    """A restore puts things back. It is not a way of making the lake match the backup."""
    _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    newer = _land(lake, "npm_access", "2026-08-03")

    plan = backup.plan_restore(lake, vault)
    assert plan.missing_from_backup == [newer]
    backup.apply_restore(lake, plan, vault)
    assert pathlib.Path(lake.local_root, newer).exists()


def test_verification_reads_the_lake_again_rather_than_trusting_the_copy(lake, vault):
    """A restore that reports success from its own return value has verified nothing."""
    lost = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, lost).unlink()

    plan = backup.plan_restore(lake, vault)
    problems = backup.verify_restore(lake, plan, vault)
    assert problems == [f"{lost}: still missing from the lake"]

    backup.apply_restore(lake, plan, vault)
    assert backup.verify_restore(lake, plan, vault) == []


def test_a_whole_lost_bucket_comes_back(lake, vault):
    """The failure the mirror exists for, end to end."""
    for table, day in (("npm_access", "2026-08-01"), ("npm_access", "2026-08-02"),
                       ("camera_events", "2026-08-01"), ("litellm_spend", "2026-08-01")):
        _land(lake, table, day, rows=5)
    backup.mirror(lake, vault)
    before = backup.inventory(lake, "raw")

    shutil.rmtree(lake.local_root)

    plan = backup.plan_restore(lake, vault)
    assert len(plan.restore) == len(before)
    backup.apply_restore(lake, plan, vault)
    assert backup.verify_restore(lake, plan, vault) == []
    assert {k: o.size for k, o in backup.inventory(lake, "raw").items()} == {
        k: o.size for k, o in before.items()
    }
