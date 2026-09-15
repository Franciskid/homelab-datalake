"""Object-level lake operations the streaming and ledger sources need.

`LakeStorage` writes a whole partition with one PUT and treats an unreadable object
as absent. That is right for a partition that is replaced on every run. It is wrong
for the three shapes added here:

* **A stream** writes many small objects per hour and later merges them, so it has to
  list what is under a prefix and delete exactly the inputs it merged.
* **A ledger** holds every version of a source table in one object and is merged on
  every run. Reading it back as "absent" when the read merely failed would rewrite the
  lake with only today's rows and lose the history the source itself has already
  forgotten. So the read here raises unless the object is genuinely not there.
* **A compacted day** of Prometheus samples is a hundred megabytes. It is built in a
  temporary file and uploaded from disk rather than held in memory as one buffer.

Kept apart from `resources.py` so the operations stay small and testable on the local
backend, which is the same code path CI exercises.
"""

from __future__ import annotations

import io
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from homelab_datalake.resources import LakeStorage


def list_keys(lake: LakeStorage, prefix: str) -> list[str]:
    """Every object key under `prefix/`, sorted. Empty when nothing is there."""
    prefix = prefix.rstrip("/")
    if lake.uses_s3:
        client = lake._client()
        keys: list[str] = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=lake.bucket, Prefix=prefix + "/"
        ):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)
    root = Path(lake.local_root)
    base = root / prefix
    if not base.exists():
        return []
    return sorted(
        str(path.relative_to(root))
        for path in base.rglob("*")
        if path.is_file() and not path.name.endswith(".tmp")
    )


def exists(lake: LakeStorage, key: str) -> bool:
    if lake.uses_s3:
        client = lake._client()
        try:
            client.head_object(Bucket=lake.bucket, Key=key)
            return True
        except client.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
    return (Path(lake.local_root) / key).is_file()


def read_strict(lake: LakeStorage, key: str) -> pa.Table | None:
    """The object as a table, None only when it does not exist.

    Any other failure raises. A merge that mistook a network error for "nothing held
    yet" would overwrite history with a single day, and nothing would say so.
    """
    if not exists(lake, key):
        return None
    if lake.uses_s3:
        body = lake._client().get_object(Bucket=lake.bucket, Key=key)["Body"].read()
        return pq.read_table(io.BytesIO(body))
    return pq.read_table(Path(lake.local_root) / key)


def put_file(lake: LakeStorage, key: str, path: str | Path) -> int:
    """Upload a local file as one object. Returns its size.

    On S3 the upload may be multipart, which MinIO still exposes only once it has
    completed, so a reader sees the previous state or the new object and never half
    of one -- the same guarantee `LakeStorage.write_table` gives.
    """
    path = Path(path)
    size = path.stat().st_size
    if lake.uses_s3:
        lake._client().upload_file(str(path), lake.bucket, key)
        return size
    destination = Path(lake.local_root) / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copyfile(path, temporary)
    temporary.replace(destination)
    return size


def delete_keys(lake: LakeStorage, keys: list[str]) -> int:
    """Delete exactly these objects. Returns how many were asked for."""
    if not keys:
        return 0
    if lake.uses_s3:
        client = lake._client()
        for start in range(0, len(keys), 1000):
            chunk = keys[start:start + 1000]
            client.delete_objects(
                Bucket=lake.bucket, Delete={"Objects": [{"Key": key} for key in chunk]}
            )
        return len(keys)
    root = Path(lake.local_root)
    for key in keys:
        (root / key).unlink(missing_ok=True)
    return len(keys)


def row_count(lake: LakeStorage, key: str) -> int:
    """Rows in one Parquet object, from its footer. Raises if it cannot be read."""
    if lake.uses_s3:
        body = lake._client().get_object(Bucket=lake.bucket, Key=key)["Body"].read()
        return pq.ParquetFile(io.BytesIO(body)).metadata.num_rows
    return pq.ParquetFile(Path(lake.local_root) / key).metadata.num_rows
