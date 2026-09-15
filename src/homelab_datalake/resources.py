"""Where a partition lands, and how the warehouse is reached.

`LakeStorage` has two backends behind one interface: MinIO in production, a local
directory in CI and local development. That split is what lets the repo be cloned
and built by someone with no access to the homelab -- the asset code is identical
either way, so CI exercises the real thing rather than a stand-in.

Which backend is used is decided by whether an S3 endpoint is configured. There is
no `mode` flag to set wrong.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


class LakeStorage(ConfigurableResource):
    """Reads and writes Parquet objects, on MinIO or on local disk.

    One object per partition, written with a single PUT. That is what makes the
    write atomic without needing the rename semantics S3 does not have: a reader
    sees either the previous object or the new one, never a half-written file.
    Re-running a partition overwrites it, which is the whole idempotency story.
    """

    endpoint: str = ""
    bucket: str = "datalake"
    access_key_id: str = ""
    secret_access_key: str = ""
    region: str = "us-east-1"
    local_root: str = ".lake"

    @property
    def uses_s3(self) -> bool:
        return bool(self.endpoint)

    def _client(self):
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key_id or None,
            aws_secret_access_key=self.secret_access_key or None,
            region_name=self.region,
        )

    def describe(self) -> str:
        return f"s3://{self.bucket}" if self.uses_s3 else f"file://{Path(self.local_root).resolve()}"

    def write_table(self, key: str, table: pa.Table) -> int:
        """Write one Parquet object. Returns bytes written."""
        buffer = io.BytesIO()
        # zstd because access logs are dominated by repetitive strings -- host,
        # user-agent, status -- which dictionary-encode first and then compress well.
        pq.write_table(table, buffer, compression="zstd", use_dictionary=True)
        payload = buffer.getvalue()

        if self.uses_s3:
            self._client().put_object(Bucket=self.bucket, Key=key, Body=payload)
        else:
            path = Path(self.local_root) / key
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_bytes(payload)
            tmp.replace(path)  # atomic on POSIX, mirroring the single-PUT guarantee
        return len(payload)

    def write_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> int:
        """Write one opaque object, as opposed to `write_table`, which always writes
        Parquet. Two callers share this, deliberately, rather than each keeping its
        own copy: a decoded camera frame or a generated file's bytes -- referenced
        *by* a Parquet row (an `object_key` or an entry in `frame_keys`) rather than
        a dataset in its own right, so there is no `layout.Dataset` for these, see
        `homelab_datalake.parsing.ai_platform` -- and an inbox fixture or any other
        non-Parquet object that a caller lands without one. `content_type` defaults
        to opaque octet-stream, so a caller that has no opinion on it need not pass
        one.

        Same atomicity story as `write_table`: one PUT on S3, a write-to-temp-then-
        rename on disk.
        """
        if self.uses_s3:
            self._client().put_object(
                Bucket=self.bucket, Key=key, Body=data, ContentType=content_type
            )
        else:
            path = Path(self.local_root) / key
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_bytes(data)
            tmp.replace(path)
        return len(data)

    def count_prefix(self, prefix: str) -> int:
        """How many objects sit under a prefix. Used to check a written count against
        an expected one without downloading anything."""
        if self.uses_s3:
            count = 0
            paginator = self._client().get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                count += len(page.get("Contents", []))
            return count
        root = Path(self.local_root) / prefix
        if not root.exists():
            return 0
        return sum(1 for path in root.rglob("*") if path.is_file())

    def delete_prefix(self, prefix: str) -> int:
        """Remove every object under a prefix. Used to make a re-run replace, not append."""
        removed = 0
        if self.uses_s3:
            client = self._client()
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                if keys:
                    client.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})
                    removed += len(keys)
        else:
            root = Path(self.local_root) / prefix
            if root.exists():
                for path in sorted(root.rglob("*")):
                    if path.is_file():
                        path.unlink()
                        removed += 1
        return removed

    def read_table(self, key: str) -> pa.Table | None:
        """One Parquet object back as an Arrow table, or None if it is not there.

        The counterpart to `write_table`, and it exists for one reason: a partition
        that is *merged* rather than replaced has to be read before it is written.
        The camera source needs that -- its upstream evicts events once its own store
        is full, so a re-read of an old day legitimately returns fewer rows than the
        lake already holds, and replacing the partition with that answer deletes
        history that no longer exists anywhere else.
        """
        try:
            if self.uses_s3:
                body = self._client().get_object(Bucket=self.bucket, Key=key)["Body"].read()
                return pq.read_table(io.BytesIO(body))
            path = Path(self.local_root) / key
            if not path.exists():
                return None
            return pq.read_table(path)
        except Exception:  # noqa: BLE001 - a missing object is the ordinary case on a
            # first write, and an unreadable one must not stop the run from landing
            # today's rows. Both mean "nothing to merge with"; the shrink check that
            # follows the merge is what catches a read that failed when it should not
            # have, because the partition would appear to lose every prior row.
            return None

    def count_rows(self, key: str) -> int | None:
        """Rows in one Parquet object, or None if it is not there.

        Reads the file footer rather than the data: Parquet stores the row count in
        its metadata, so this is a couple of range requests regardless of file size.
        Used to compare a snapshot against the previous one without loading either.
        """
        import pyarrow.parquet as pq

        try:
            if self.uses_s3:
                body = self._client().get_object(Bucket=self.bucket, Key=key)["Body"].read()
                return pq.ParquetFile(io.BytesIO(body)).metadata.num_rows
            path = Path(self.local_root) / key
            if not path.exists():
                return None
            return pq.ParquetFile(path).metadata.num_rows
        except Exception:  # noqa: BLE001 - a missing or unreadable prior snapshot is
            # not an error: it means there is nothing to compare against, and the
            # caller treats None as "first snapshot".
            return None

    def duckdb_scan_target(self, prefix: str) -> str:
        """A glob DuckDB can read every partition through."""
        if self.uses_s3:
            return f"s3://{self.bucket}/{prefix}/**/*.parquet"
        return str((Path(self.local_root) / prefix).resolve() / "**" / "*.parquet")

    def duckdb_settings(self) -> list[tuple[str, str]]:
        """The DuckDB settings that point at this lake, as (name, value).

        Separated from applying them so the *shape* of the configuration can be
        tested without a bucket -- the failure this guards against is silent on a
        local backend, where none of these settings exist at all.
        """
        if not self.uses_s3:
            return []
        host = self.endpoint.replace("https://", "").replace("http://", "")
        return [
            ("s3_endpoint", host),
            ("s3_region", self.region),
            ("s3_access_key_id", self.access_key_id),
            ("s3_secret_access_key", self.secret_access_key),
            # MinIO is path-style, not virtual-host.
            ("s3_url_style", "path"),
            ("s3_use_ssl", "true" if self.endpoint.startswith("https") else "false"),
        ]

    def configure_duckdb(self, con, scope: str = "") -> None:
        """Teach a DuckDB connection how to reach the lake, if it is on S3.

        `scope` is either empty or "GLOBAL", and the difference is load-bearing for
        any caller that runs queries on a *cursor* rather than on the connection
        itself. Every s3_* setting is LOCAL by default, and a cursor inherits none
        of its parent's local settings -- measured against the real bucket:

            parent  s3_endpoint  LOCAL   s3.homelab.example.test
            cursor  s3_endpoint  GLOBAL  None

        so the cursor read the bucket anonymously and got a 403 that reads as
        "invalid or missing credentials" with the credentials plainly configured.
        SET GLOBAL puts them on the database instead of the session, where a cursor
        does see them. It does not loosen anything: lock_configuration still refuses
        every later change, on the cursor as well as the connection.
        """
        if not self.uses_s3:
            return
        con.execute("INSTALL httpfs; LOAD httpfs;")
        prefix = f"SET {scope} " if scope else "SET "
        for name, value in self.duckdb_settings():
            # The values come from configuration, never from a caller, and are
            # escaped anyway: a stray quote in an env var would otherwise end the
            # string literal and change the statement.
            con.execute(f"{prefix}{name}='{value.replace(chr(39), chr(39) * 2)}';")

    @classmethod
    def from_env(cls) -> LakeStorage:
        return cls(
            endpoint=os.environ.get("DATALAKE_S3_ENDPOINT", ""),
            bucket=os.environ.get("DATALAKE_S3_BUCKET", "datalake"),
            access_key_id=os.environ.get("DATALAKE_S3_ACCESS_KEY_ID", ""),
            secret_access_key=os.environ.get("DATALAKE_S3_SECRET_ACCESS_KEY", ""),
            region=os.environ.get("DATALAKE_S3_REGION", "us-east-1"),
            local_root=os.environ.get("DATALAKE_LOCAL_ROOT", ".lake"),
        )


class NpmLogSources(ConfigurableResource):
    """Where each host's NPM logs are readable from, inside the Dagster container.

    primary's are a read-only bind mount of the live NPM volume. Only the instances
    actually mounted are ingested -- NPM_HOSTS decides, and it defaults to primary
    alone. The two sites are never joined at the host level; anything read across
    them goes over its public URL with a token, as the camera source does.
    """

    # ConfigurableResource is a Pydantic model, which deep-copies field
    # defaults per instance, so this shared-mutable-default is not one.
    directories: dict[str, str] = {}  # noqa: RUF012

    def directory_for(self, host: str) -> str:
        try:
            return self.directories[host]
        except KeyError:
            raise KeyError(
                f"no log directory configured for host {host!r}; "
                f"known hosts: {sorted(self.directories)}"
            ) from None

    @classmethod
    def from_env(cls) -> NpmLogSources:
        return cls(
            directories={
                "primary": os.environ.get("NPM_LOG_DIR_PRIMARY", "./seed/logs/primary"),
                "secondary": os.environ.get("NPM_LOG_DIR_SECONDARY", "./seed/logs/secondary"),
            }
        )
