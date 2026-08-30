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


def _land(lake: LakeStorage, table: str, day: str, rows: int = 1, host: str = "primary") -> str:
    """Write one object at the key production actually uses.

    The npm datasets partition on two dimensions, and the host comes *first*:

        raw/infrastructure/npm/access/host=primary/dt=2026-07-27/data.parquet

    An earlier version of this helper wrote `<prefix>/<partition_column>=<day>/` for
    everything, and that single simplification hid a restore that could not find an
    npm partition at all -- it reported "0 objects to restore" while the object sat
    in the backup. A fixture that does not reproduce the real key shape is a fixture
    that tests a layout nothing writes.
    """
    dataset = layout.dataset(table)
    parts = [dataset.prefix]
    if table.startswith("npm_"):
        parts.append(f"host={host}")
    parts.append(f"{dataset.partition_column}={day}")
    key = "/".join([*parts, "data.parquet"])
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


def test_an_orphan_the_key_cannot_delete_is_reported_not_raised(lake, vault, monkeypatch):
    """The deployed policy grants no DeleteObject on the backup, on purpose.

    A pipeline able to erase its own backup is not backed up. So retention is a
    policy this code expresses and the bucket declines to enforce -- and the asset
    must say so rather than turn a deliberate permission boundary into a red nightly
    job every night for ever.
    """
    key = _land(lake, "npm_access", "2026-08-01")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, key).unlink()

    def refuse(_storage, _key):
        raise PermissionError("AccessDenied")

    monkeypatch.setattr(backup, "_delete", refuse)
    result = backup.mirror(lake, vault, orphan_days=90,
                           now=datetime.now(UTC) + timedelta(days=91))

    assert result.orphans == [key]
    assert result.expired == 0
    assert result.unexpired == 1
    # And the object is still there, which is the whole point.
    assert key in backup.inventory(vault, "raw")


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


def test_a_day_is_a_segment_to_match_not_a_prefix_to_append():
    """npm partitions on host *and* day, and the host comes first in the key.

    `<prefix>/dt=<day>` matches nothing there, and matching nothing is reported as
    "0 objects to restore" -- which during an incident reads as the backup being
    empty. Found by running the drill against the real bucket.
    """
    assert backup.day_segment("npm_access", "2026-07-27") == "dt=2026-07-27"
    assert backup.day_segment("camera_events", "2026-08-01") == "dt=2026-08-01"


def test_one_day_of_a_doubly_partitioned_dataset_can_be_restored(lake, vault):
    """The bug the drill found, as a test: two hosts, one day, one host lost."""
    kept = _land(lake, "npm_access", "2026-07-27", host="secondary")
    lost = _land(lake, "npm_access", "2026-07-27", host="primary", rows=99)
    other_day = _land(lake, "npm_access", "2026-07-28", host="primary")
    backup.mirror(lake, vault)
    pathlib.Path(lake.local_root, lost).unlink()

    scope = backup.restore_scope("npm_access")
    segment = backup.day_segment("npm_access", "2026-07-27")
    plan = backup.plan_restore(lake, vault, scope, segment)

    assert [o.key for o in plan.restore] == [lost]
    assert plan.identical == [kept]
    backup.apply_restore(lake, plan, vault)
    assert backup.verify_restore(lake, plan, vault, scope, segment) == []
    assert pathlib.Path(lake.local_root, lost).exists()
    # The other day was never in scope and is untouched.
    assert pathlib.Path(lake.local_root, other_day).exists()


def test_a_derived_dataset_cannot_be_restored_because_it_is_not_backed_up():
    """Silently restoring nothing would look exactly like a successful restore."""
    with pytest.raises(ValueError, match="rebuilt from raw"):
        backup.restore_scope("agg_traffic_daily")


def test_an_unpartitioned_dataset_refuses_a_day():
    with pytest.raises(ValueError, match="not partitioned"):
        backup.day_segment("tmdb_titles", "2026-08-01")


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

    plan = backup.plan_restore(lake, vault, backup.restore_scope("npm_access"),
                               backup.day_segment("npm_access", "2026-08-01"))
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
