"""Moving the lake from the flat layout to the layered one, without losing a byte.

The old layout was `raw/<table>/...`. The new one is
`<layer>/<domain>/<source>/<dataset>/...`, and the pairs come from
homelab_datalake.layout so this file cannot disagree with the code that writes.

The order is copy, verify, then delete, and never anything else. An object is only
removed after its copy has been listed at the new key with the same size. If the
process dies halfway, re-running it finishes the job: a copy that already exists is
identical, because these objects are immutable once written.

On MinIO the copy is server-side -- the bytes never come to this process -- so 333
objects and six megabytes move in a few seconds. On the local backend used by tests
and CI it is a rename, for the same reason.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from homelab_datalake import layout
from homelab_datalake.resources import LakeStorage

logger = logging.getLogger(__name__)


@dataclass
class Move:
    old_key: str
    new_key: str
    size: int


@dataclass
class Plan:
    moves: list[Move] = field(default_factory=list)
    # Objects already at their new key. Not an error and not re-copied: this is what
    # a second run of an interrupted migration sees.
    already_moved: int = 0
    # Objects under no known legacy prefix. Reported rather than touched -- deleting
    # something the registry does not recognise is how a migration loses data.
    unknown: list[str] = field(default_factory=list)

    @property
    def bytes_to_move(self) -> int:
        return sum(m.size for m in self.moves)


def _relocate(key: str, old_prefix: str, new_prefix: str) -> str:
    """Swap the prefix, keep everything after it -- the partition path is unchanged."""
    return new_prefix + key[len(old_prefix):]


def plan(objects: dict[str, int]) -> Plan:
    """Decide what moves where, from a mapping of key to size.

    Pure, so the decision can be tested without a bucket. `objects` is every key in
    the lake; anything the registry does not claim comes back in `unknown`.
    """
    pairs = layout.migrations()
    new_prefixes = tuple(d.prefix for d in layout.ALL_DATASETS)
    result = Plan()

    for key in sorted(objects):
        for old_prefix, new_prefix in pairs:
            if key.startswith(old_prefix + "/"):
                result.moves.append(
                    Move(key, _relocate(key, old_prefix, new_prefix), objects[key])
                )
                break
        else:
            if any(key.startswith(p + "/") for p in new_prefixes):
                result.already_moved += 1
            else:
                result.unknown.append(key)
    return result


def _list_s3(lake: LakeStorage) -> dict[str, int]:
    client = lake._client()
    found: dict[str, int] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=lake.bucket):
        for obj in page.get("Contents", []):
            found[obj["Key"]] = obj["Size"]
    return found


def _list_local(lake: LakeStorage) -> dict[str, int]:
    root = Path(lake.local_root)
    if not root.exists():
        return {}
    return {
        str(p.relative_to(root)): p.stat().st_size
        for p in root.rglob("*")
        if p.is_file()
    }


def inventory(lake: LakeStorage) -> dict[str, int]:
    return _list_s3(lake) if lake.uses_s3 else _list_local(lake)


def apply(lake: LakeStorage, migration: Plan) -> int:
    """Copy, verify, delete. Returns the number of objects moved.

    Verification is a fresh listing of the new key rather than trusting the copy's
    return value, and the size must match. Only then is the old key removed, one
    object at a time -- a batch delete issued before the whole batch was verified
    would be the one way this could lose data.
    """
    moved = 0
    if lake.uses_s3:
        client = lake._client()
        for move in migration.moves:
            client.copy_object(
                Bucket=lake.bucket,
                Key=move.new_key,
                CopySource={"Bucket": lake.bucket, "Key": move.old_key},
            )
            landed = client.head_object(Bucket=lake.bucket, Key=move.new_key)
            if landed["ContentLength"] != move.size:
                raise RuntimeError(
                    f"{move.new_key} is {landed['ContentLength']} bytes but "
                    f"{move.old_key} was {move.size}. Nothing was deleted; fix the "
                    "copy and run again."
                )
            client.delete_object(Bucket=lake.bucket, Key=move.old_key)
            moved += 1
    else:
        root = Path(lake.local_root)
        for move in migration.moves:
            source = root / move.old_key
            target = root / move.new_key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            if target.stat().st_size != move.size:
                raise RuntimeError(f"{target} is the wrong size after copy")
            source.unlink()
            moved += 1
        # Directories left behind by the moves. Only empty ones, and only under a
        # legacy prefix, so this cannot reach anything the plan did not claim.
        for old_prefix, _ in layout.migrations():
            stale = root / old_prefix
            if stale.exists() and not any(p.is_file() for p in stale.rglob("*")):
                shutil.rmtree(stale)
    return moved
