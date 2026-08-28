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

    def duckdb_scan_target(self, prefix: str) -> str:
        """A glob DuckDB can read every partition through."""
        if self.uses_s3:
            return f"s3://{self.bucket}/{prefix}/**/*.parquet"
        return str((Path(self.local_root) / prefix).resolve() / "**" / "*.parquet")

    def configure_duckdb(self, con) -> None:
        """Teach a DuckDB connection how to reach the lake, if it is on S3."""
        if not self.uses_s3:
            return
        host = self.endpoint.replace("https://", "").replace("http://", "")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{host}';")
        con.execute(f"SET s3_region='{self.region}';")
        con.execute(f"SET s3_access_key_id='{self.access_key_id}';")
        con.execute(f"SET s3_secret_access_key='{self.secret_access_key}';")
        con.execute("SET s3_url_style='path';")  # MinIO is path-style, not virtual-host
        con.execute(f"SET s3_use_ssl={'true' if self.endpoint.startswith('https') else 'false'};")

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

    primary's are a read-only bind mount of the live NPM volume. secondary's
    are a nightly rsync (scripts/sync-secondary-logs.sh) because that host is a different
    machine -- see IMPLEMENTATION_PLAN.md section 2.
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
