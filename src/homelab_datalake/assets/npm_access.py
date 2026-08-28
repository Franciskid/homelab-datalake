"""NPM access-log assets.

Partitioned on two dimensions, date and host, because a day is only half of an
address here: the two NPM instances live on different machines and fail
independently. `(2026-08-27, secondary)` being broken should not mark
`(2026-08-27, primary)` broken too, and re-running one must not touch the other.
That is what a MultiPartitionsDefinition buys, and it is why the object key
carries both.
"""

import os
from datetime import date

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    MultiPartitionsDefinition,
    Output,
    RetryPolicy,
    StaticPartitionsDefinition,
    asset,
)

from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.extract import extract_partition
from homelab_datalake.resources import LakeStorage, NpmLogSources

# 27 July 2026 is the oldest line the servers still hold. NPM rotates weekly and
# keeps four generations, so there is roughly a five-week window and no more --
# backfilling past this start date would produce empty partitions, not errors,
# which is worse. See IMPLEMENTATION_PLAN.md section 6.
LOG_RETENTION_START = "2026-07-27"

HOSTS = ["primary", "secondary"]

npm_partitions = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date=LOG_RETENTION_START),
        "host": StaticPartitionsDefinition(HOSTS),
    }
)

RAW_PREFIX = "raw/npm_access"
REJECT_PREFIX = "raw/npm_access_rejects"

# Above this, the parser has stopped understanding its input and the run should fail
# rather than quietly deliver a partial day. Measured reject rate on [count withheld] real
# lines across both hosts was [rate withheld], so anything approaching [rate withheld] is a real change.
MAX_REJECT_RATE = 0.01

_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("host_source", pa.string()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("log_format", pa.string()),
        ("request_host", pa.string()),
        ("method", pa.string()),
        ("scheme", pa.string()),
        ("path", pa.string()),
        ("status", pa.int32()),
        ("upstream_status", pa.string()),
        ("cache_status", pa.string()),
        ("bytes_sent", pa.int64()),
        ("user_agent", pa.string()),
        ("sent_to", pa.string()),
        ("client_hash", pa.string()),
        ("client_network", pa.string()),
        ("client_trust", pa.string()),
        ("client_version", pa.int32()),
        ("client_is_private", pa.bool_()),
        ("geo_country_code", pa.string()),
        ("geo_country_name", pa.string()),
        ("geo_city", pa.string()),
        ("geo_asn", pa.int64()),
        ("geo_as_org", pa.string()),
        ("source_file", pa.string()),
    ]
)

_REJECT_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("host", pa.string()),
        ("source_file", pa.string()),
        ("reason", pa.string()),
        ("raw_line", pa.string()),
    ]
)


def partition_keys(context: AssetExecutionContext) -> tuple[date, str]:
    keys = context.partition_key.keys_by_dimension
    return date.fromisoformat(keys["date"]), keys["host"]


def _object_key(prefix: str, host: str, day: date) -> str:
    return f"{prefix}/host={host}/dt={day.isoformat()}/data.parquet"


CHECK_REJECT_RATE = "reject_rate_within_threshold"
CHECK_NOT_EMPTY = "partition_is_not_silently_empty"


@asset(
    partitions_def=npm_partitions,
    group_name="npm",
    compute_kind="python",
    # Declared here rather than as standalone @asset_check functions. A standalone
    # check takes the asset's value as an input, which for a partitioned asset means
    # loading every partition back through the IO manager just to re-derive numbers
    # this run already has. Yielding the results inline keeps each one scoped to the
    # partition that produced it.
    check_specs=[
        AssetCheckSpec(
            name=CHECK_REJECT_RATE,
            asset="npm_access_raw",
            description=(
                "Fails when too many lines could not be parsed. A rising reject rate "
                "means NPM changed its log format and the pipeline is silently losing "
                "traffic -- the failure a green dashboard would hide."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_NOT_EMPTY,
            asset="npm_access_raw",
            description=(
                "Fails when a partition read lines but produced no rows and excluded "
                "none. An empty day is normal; an empty day that read thousands of "
                "lines is a bug in the day filter."
            ),
        ),
    ],
    # Reading a mounted file should fail loudly: a missing mount will not fix itself,
    # and retrying only hides it. The retries here cover the object-store write.
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description=(
        "One day of one host's NPM access logs, parsed, geo-enriched and redacted, "
        "landed as a single Parquet object. Client addresses are hashed before the "
        "write; no raw address ever reaches storage."
    ),
)
def npm_access_raw(
    context: AssetExecutionContext,
    lake: LakeStorage,
    npm_logs: NpmLogSources,
):
    day, host = partition_keys(context)
    log_dir = npm_logs.directory_for(host)

    salt = os.environ.get("CLIENT_IP_HASH_SALT", "")
    if not salt:
        # Failing here is deliberate. Defaulting to an empty salt would write
        # reversible hashes of real addresses, which is the exact outcome the
        # privacy boundary exists to prevent -- and it would do it silently.
        raise ValueError(
            "CLIENT_IP_HASH_SALT is not set. Refusing to run rather than write "
            "reversible client hashes. Generate one with: openssl rand -hex 32"
        )

    if not os.path.isdir(log_dir):
        raise FileNotFoundError(
            f"log directory {log_dir!r} for host {host!r} is not readable. "
            "On primary this is a bind mount of NPM's /data/logs; for secondary it is "
            "populated by scripts/sync-secondary-logs.sh."
        )

    geo = GeoEnricher.from_env()
    result = extract_partition(log_dir, day, host, salt, geo)
    context.log.info(
        "host=%s dt=%s read %d files (%d pruned by mtime), %d lines -> %d rows, %d rejects",
        host, day, result.files_read, result.files_skipped,
        result.stats.total, len(result.rows), len(result.rejects),
    )

    data_key = _object_key(RAW_PREFIX, host, day)
    reject_key = _object_key(REJECT_PREFIX, host, day)

    # Replace, never append. Deleting the prefix first means a re-run that produces
    # fewer rows -- because a rotated file aged out -- does not leave the old ones behind.
    lake.delete_prefix(f"{RAW_PREFIX}/host={host}/dt={day.isoformat()}")
    lake.delete_prefix(f"{REJECT_PREFIX}/host={host}/dt={day.isoformat()}")

    table = pa.Table.from_pylist(result.rows, schema=_SCHEMA)
    written = lake.write_table(data_key, table)
    reject_table = pa.Table.from_pylist(result.rejects, schema=_REJECT_SCHEMA)
    lake.write_table(reject_key, reject_table)

    reject_rate = result.stats.reject_rate
    yield Output(
        {
            "rows": len(result.rows),
            "rejects": len(result.rejects),
            "lines_read": result.stats.total,
            "reject_rate": reject_rate,
            "out_of_partition": result.out_of_partition,
            "geo_enabled": result.geo_enabled,
        },
        metadata={
            "rows": len(result.rows),
            "lines_read": result.stats.total,
            "rejects": len(result.rejects),
            "reject_rate": MetadataValue.float(round(reject_rate, 6)),
            "lines_outside_partition": result.out_of_partition,
            "formats": MetadataValue.json(result.stats.by_format),
            "files_read": result.files_read,
            "files_pruned_by_mtime": result.files_skipped,
            "geo_enrichment": "on" if result.geo_enabled else "off (no MaxMind database)",
            "bytes_written": written,
            "destination": MetadataValue.path(f"{lake.describe()}/{data_key}"),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_REJECT_RATE,
        passed=reject_rate <= MAX_REJECT_RATE,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "reject_rate": MetadataValue.float(round(reject_rate, 6)),
            "threshold": MAX_REJECT_RATE,
            "rejected_lines": len(result.rejects),
            "lines_read": result.stats.total,
        },
    )

    # "Read lines but produced nothing, and excluded nothing" is the signature of a
    # broken day filter. A genuinely quiet day reads few lines; a day whose lines all
    # belong to other partitions reports them in out_of_partition. Neither trips this.
    silently_empty = (
        result.stats.total > 0 and not result.rows and result.out_of_partition == 0
    )
    yield AssetCheckResult(
        check_name=CHECK_NOT_EMPTY,
        passed=not silently_empty,
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": len(result.rows), "lines_read": result.stats.total},
    )
