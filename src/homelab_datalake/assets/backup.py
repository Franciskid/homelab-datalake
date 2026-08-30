"""A second copy of the raw layer, and the restore that proves it is one.

The warehouse is a build artefact: lose it and `warehouse_tables` plus `dbt build`
reconstructs it from the lake. The *lake* is not, and only part of it is even
rebuildable from itself. `curated/` and `marts/` come back from `raw/` by re-running
`analytics_daily`, so copying them would be copying a derivation. `raw/` is the
original, and its sources will not produce it again -- the reverse-proxy logs rotate
off the disk within days, and the camera serves only a recent window. A raw partition
that is deleted is gone.

That was tolerable while nothing could write to the lake. The workbench's switch
changes it, which is why this exists before the switch is released rather than after
the first accident.

**Three layers of protection, and each covers what the one before it does not.**

    bucket versioning    an overwrite or a delete of one object, undone in seconds
    this mirror          the bucket itself lost, or versioning turned off
    the restore script   the only one of the three that has ever been exercised

Versioning is set up once by `scripts/enable-lake-versioning.sh`; this module is the
second layer.

**Why a mirror rather than dated snapshots.** A snapshot per day of a layer that only
ever grows would store the same objects again every night. The lake objects are
immutable once written -- a partition is replaced by a whole new PUT, never edited --
so a key that is already in the backup with the same size and ETag is already the
right bytes, and copying it again buys nothing. The copy is server-side on MinIO, so
the bytes never travel through this process.

**What happens to an object that leaves raw/.** It stays in the backup, because that
is the case this whole module exists for: the copy has to outlive the deletion or it
is not a backup. It is reported on every run, and removed only after a long retention
window -- long enough that a deletion nobody noticed for a month is still recoverable,
bounded so a mirror of a bucket that is being churned cannot grow without limit.

The restore lives here too rather than in the script that calls it. It is the inverse
of the same key mapping, and a restore that is written somewhere the tests do not
reach is exactly the kind of backup nobody has ever run.
"""

# No `from __future__ import annotations`: Dagster resolves the `context` annotation
# at decoration time, and a string annotation makes that check fail with a message
# that says the type is wrong when the type is right.
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from homelab_datalake import layout
from homelab_datalake.assets.camera import camera_events_raw
from homelab_datalake.assets.jellyfin import jellyfin_library_raw
from homelab_datalake.assets.litellm_spend import litellm_spend_raw
from homelab_datalake.assets.npm_access import npm_access_raw
from homelab_datalake.assets.tmdb import tmdb_titles_raw
from homelab_datalake.resources import LakeStorage

logger = logging.getLogger(__name__)

# Only raw/. curated/ and marts/ are rebuilt from it by analytics_daily, so a copy of
# them costs storage and buys nothing a re-run would not.
BACKUP_PREFIX = layout.LAYER_RAW

# How long an object that has left raw/ is kept in the backup before it is removed.
# Ninety days rather than thirty: the failure this guards against is a deletion
# nobody noticed, and nobody notices in a week. Set it to 0 to keep orphans forever.
ORPHAN_RETENTION_DAYS = int(os.environ.get("DATALAKE_BACKUP_ORPHAN_DAYS", "90"))


@dataclass
class Object:
    key: str
    size: int
    modified: datetime
    # ETag on S3, and None on the local backend, where there is nothing to compare
    # against and size plus name is what is available. Used only to decide whether a
    # key that is already in the backup needs copying again.
    etag: str | None = None


@dataclass
class MirrorResult:
    copied: int = 0
    bytes_copied: int = 0
    skipped: int = 0
    # In the backup, no longer in raw/. Reported every run: this is either the
    # ordinary result of a partition being renamed, or the first sign of data loss,
    # and telling them apart is a person's job.
    orphans: list[str] = field(default_factory=list)
    expired: int = 0

    @property
    def total(self) -> int:
        return self.copied + self.skipped


@dataclass
class RestorePlan:
    """What a restore would put back. Built, printed, and only then carried out."""

    restore: list[Object] = field(default_factory=list)
    # Present in both, byte-for-byte identical. Not restored, and not an error: this
    # is what most of a partial restore looks like.
    identical: list[str] = field(default_factory=list)
    # Present in both and different. Restoring overwrites the live object, which is
    # the point, but it is the line of the report to read twice.
    differing: list[str] = field(default_factory=list)
    missing_from_backup: list[str] = field(default_factory=list)

    @property
    def bytes_to_restore(self) -> int:
        return sum(o.size for o in self.restore)


def backup_bucket() -> str:
    return os.environ.get("DATALAKE_BACKUP_BUCKET", "datalake-backup")


def backup_storage(lake: LakeStorage) -> LakeStorage:
    """The same MinIO, a different bucket -- or a sibling directory on disk.

    Deliberately not a different host. This copy defends against a delete, an
    overwrite and a dropped bucket; it does not defend against losing the machine,
    and pretending otherwise by putting it in a directory called `offsite` would be
    worse than saying so.
    """
    if lake.uses_s3:
        return lake.model_copy(update={"bucket": backup_bucket()})
    return lake.model_copy(update={"local_root": str(Path(lake.local_root).with_name(
        Path(lake.local_root).name + "-backup"))})


# --------------------------------------------------------------------------------
# listing, on either backend
# --------------------------------------------------------------------------------


def inventory(storage: LakeStorage, prefix: str) -> dict[str, Object]:
    """Every object under a prefix, with what is needed to compare two copies."""
    found: dict[str, Object] = {}
    if storage.uses_s3:
        client = storage._client()
        pages = client.get_paginator("list_objects_v2")
        try:
            for page in pages.paginate(Bucket=storage.bucket, Prefix=prefix + "/"):
                for obj in page.get("Contents", []):
                    found[obj["Key"]] = Object(
                        key=obj["Key"], size=obj["Size"], modified=obj["LastModified"],
                        etag=obj["ETag"].strip('"'),
                    )
        except client.exceptions.NoSuchBucket:
            # The backup bucket has not been created yet. An empty inventory is the
            # honest description of that, and the mirror below creates it.
            return {}
        return found

    root = Path(storage.local_root)
    for path in sorted((root / prefix).rglob("*")):
        if path.is_file():
            stat = path.stat()
            found[str(path.relative_to(root))] = Object(
                key=str(path.relative_to(root)), size=stat.st_size,
                modified=datetime.fromtimestamp(stat.st_mtime, UTC),
            )
    return found


def _same(left: Object, right: Object) -> bool:
    """Whether the backup already holds these bytes.

    Size and ETag, not a re-read. Lake objects are immutable once written -- a
    partition is replaced by a whole new PUT, never edited -- so a key that matches on
    both is the object, and comparing content would mean downloading the lake nightly
    to learn nothing.
    """
    if left.size != right.size:
        return False
    if left.etag is None or right.etag is None:
        return True
    return left.etag == right.etag


def _copy(source: LakeStorage, target: LakeStorage, key: str) -> None:
    if source.uses_s3:
        client = source._client()
        # Server-side: MinIO moves the bytes internally and this process never sees
        # them. A [volume withheld] lake is not the reason -- doing it any other way would make
        # the nightly job's duration a function of the lake's size and the link's.
        client.copy_object(
            Bucket=target.bucket, Key=key,
            CopySource={"Bucket": source.bucket, "Key": key},
        )
        return
    destination = Path(target.local_root) / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes((Path(source.local_root) / key).read_bytes())


def _delete(storage: LakeStorage, key: str) -> None:
    if storage.uses_s3:
        storage._client().delete_object(Bucket=storage.bucket, Key=key)
        return
    (Path(storage.local_root) / key).unlink(missing_ok=True)


def _ensure_bucket(storage: LakeStorage) -> None:
    """The backup bucket has to exist. This asset cannot make it.

    Measured: the datalake access key gets AccessDenied on CreateBucket, the same
    way it does on GetBucketVersioning. That is correct -- a key the pipeline uses
    every night should not be able to create or reconfigure buckets -- so the
    bucket is made once, with the MinIO root credentials, by
    scripts/enable-lake-versioning.sh.

    The failure is deliberately loud rather than a skip. A backup asset that
    quietly does nothing when its destination is missing is the exact shape of the
    problem this module exists to prevent: it would stay green for months and be
    discovered empty on the day it was needed.
    """
    if not storage.uses_s3:
        Path(storage.local_root).mkdir(parents=True, exist_ok=True)
        return
    client = storage._client()
    try:
        client.head_bucket(Bucket=storage.bucket)
        return
    except Exception as absent:  # noqa: BLE001 - head_bucket raises several unrelated
        # types for "not there", and trying to create it is the right answer to all
        # of them. Logged at debug rather than swallowed: on the ordinary path this
        # fires once, on the first night, and says nothing an operator needs.
        logger.debug("%s is not reachable (%s); trying to create it", storage.bucket, absent)
    try:
        client.create_bucket(Bucket=storage.bucket)
    except Exception as error:  # noqa: BLE001 - reported, with the fix, whatever it is
        raise RuntimeError(
            f"the backup bucket {storage.bucket!r} does not exist and this key cannot "
            f"create one ({type(error).__name__}: {error}).\n\n"
            "The datalake key is deliberately not an admin key. Create the bucket once "
            "with the MinIO root credentials:\n\n"
            "    bash scripts/enable-lake-versioning.sh --apply\n\n"
            "That script creates it, turns versioning on for both buckets, and adds the "
            "lifecycle rule that stops old versions accumulating. Until it has run, raw/ "
            "has no second copy and the write switch must stay shut."
        ) from None


# --------------------------------------------------------------------------------
# the mirror
# --------------------------------------------------------------------------------


def mirror(
    lake: LakeStorage,
    target: LakeStorage | None = None,
    prefix: str = BACKUP_PREFIX,
    orphan_days: int = ORPHAN_RETENTION_DAYS,
    now: datetime | None = None,
) -> MirrorResult:
    """Bring the backup up to date with the raw layer. Idempotent."""
    target = target or backup_storage(lake)
    now = now or datetime.now(UTC)
    _ensure_bucket(target)

    live = inventory(lake, prefix)
    held = inventory(target, prefix)
    result = MirrorResult()

    for key, obj in sorted(live.items()):
        existing = held.get(key)
        if existing is not None and _same(obj, existing):
            result.skipped += 1
            continue
        _copy(lake, target, key)
        result.copied += 1
        result.bytes_copied += obj.size

    for key, obj in sorted(held.items()):
        if key in live:
            continue
        result.orphans.append(key)
        # Nothing is removed when retention is off, and nothing is removed inside the
        # window. The window is the point: a partition deleted by accident has to
        # still be here when somebody finally looks.
        if orphan_days > 0 and obj.modified < now - timedelta(days=orphan_days):
            _delete(target, key)
            result.expired += 1

    return result


# --------------------------------------------------------------------------------
# the restore
# --------------------------------------------------------------------------------


def restore_scope(dataset: str | None = None, day: str | None = None) -> str:
    """The prefix a restore covers: everything, one dataset, or one of its days.

    The dataset's prefix comes from the registry rather than being assembled here,
    so a restore cannot be pointed at a path the publisher never writes to -- which
    would report "0 objects" and look like a successful restore of nothing.
    """
    if dataset is None:
        return BACKUP_PREFIX
    entry = layout.dataset(dataset)
    if entry.layer != layout.LAYER_RAW:
        raise ValueError(
            f"{dataset!r} is in the {entry.layer!r} layer, which is not backed up: it "
            "is rebuilt from raw/ by analytics_daily. Re-run that job instead."
        )
    if day is None:
        return entry.prefix
    if entry.partition_column is None:
        raise ValueError(f"{dataset!r} is not partitioned by day, so --day does not apply.")
    return f"{entry.prefix}/{entry.partition_column}={day}"


def plan_restore(lake: LakeStorage, target: LakeStorage | None = None,
                 prefix: str = BACKUP_PREFIX) -> RestorePlan:
    """What a restore of this prefix would do. Reads both sides; changes neither."""
    target = target or backup_storage(lake)
    held = inventory(target, prefix)
    live = inventory(lake, prefix)
    plan = RestorePlan()

    for key, obj in sorted(held.items()):
        current = live.get(key)
        if current is None:
            plan.restore.append(obj)
        elif _same(obj, current):
            plan.identical.append(key)
        else:
            plan.differing.append(key)
            plan.restore.append(obj)

    plan.missing_from_backup = sorted(set(live) - set(held))
    return plan


def apply_restore(lake: LakeStorage, plan: RestorePlan,
                  target: LakeStorage | None = None) -> int:
    """Copy the planned objects back into the lake. Returns how many."""
    target = target or backup_storage(lake)
    for obj in plan.restore:
        _copy(target, lake, obj.key)
    return len(plan.restore)


def verify_restore(lake: LakeStorage, plan: RestorePlan,
                   target: LakeStorage | None = None,
                   prefix: str = BACKUP_PREFIX) -> list[str]:
    """Re-list both sides and report anything that still disagrees.

    A restore that reports success from its own return value has verified nothing.
    This reads the lake again afterwards and compares it to the backup, which is the
    only statement worth making: every object that was in the backup is now in the
    lake, at the same size.
    """
    held = inventory(target or backup_storage(lake), prefix)
    live = inventory(lake, prefix)
    problems = []
    for key, obj in sorted(held.items()):
        current = live.get(key)
        if current is None:
            problems.append(f"{key}: still missing from the lake")
        elif not _same(obj, current):
            problems.append(
                f"{key}: {current.size} bytes in the lake, {obj.size} in the backup"
            )
    return problems


# --------------------------------------------------------------------------------
# the asset
# --------------------------------------------------------------------------------


@asset(
    deps=[npm_access_raw, litellm_spend_raw, jellyfin_library_raw,
          camera_events_raw, tmdb_titles_raw],
    group_name="warehouse",
    compute_kind="s3",
    description=(
        "Mirrors raw/ into a second bucket. Only raw/: curated/ and marts/ are "
        "rebuilt from it, so a copy of them buys nothing a re-run would not. In the "
        "warehouse group so it runs with the nightly job and is visible in the same "
        "graph as the layers it protects."
    ),
)
def lake_backup(context: AssetExecutionContext, lake: LakeStorage) -> Output[dict]:
    target = backup_storage(lake)
    result = mirror(lake, target)

    context.log.info(
        "backup: %d copied (%.2f MB), %d already current, %d orphaned, %d expired -> %s",
        result.copied, result.bytes_copied / 1e6, result.skipped,
        len(result.orphans), result.expired, target.describe(),
    )
    if result.orphans:
        # Loud, and not an error. It is the ordinary result of a rename and the first
        # sign of data loss, and only a person can tell those apart.
        context.log.warning(
            "%d object(s) are in the backup and no longer in raw/. If this was not a "
            "deliberate change, restore before %d days pass: %s",
            len(result.orphans), ORPHAN_RETENTION_DAYS, ", ".join(result.orphans[:10]),
        )

    return Output(
        {"copied": result.copied, "objects": result.total},
        metadata={
            "objects_in_backup": result.total,
            "copied": result.copied,
            "already_current": result.skipped,
            "bytes_copied": result.bytes_copied,
            "orphaned": len(result.orphans),
            "expired": result.expired,
            "target": MetadataValue.path(f"{target.describe()}/{BACKUP_PREFIX}"),
        },
    )
