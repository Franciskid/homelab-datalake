"""NPM access-log assets, plus mcp.ai's own access log landed in the same table.

Partitioned on two dimensions, date and host, because a day is only half of an
address here: primary and secondary run on different machines and fail independently.
`(2026-08-27, secondary)` being broken should not mark `(2026-08-27, primary)` broken too,
and re-running one must not touch the other. That is what a MultiPartitionsDefinition
buys, and it is why the object key carries both.

secondary's lines reach `npm_access_raw` differently from primary's, since 2026-09-14
(WS-41.E2 part B): secondary is not reachable from this host, so its logs are
mirrored hourly into the lake bucket by `infra/secondary-log-sync/` and read from there
through `extract_partition_from_lake`, instead of a local bind mount. primary keeps
reading a mounted directory. Both still land in the same `raw.npm_access` table,
through the same schema and the same checks -- only where the bytes come from
differs.

`mcp_api_access_raw` below is a second, independent asset over the *same* lake
prefix (WS-41.E2 part C): mcp.ai.example.test is not behind NPM at all, so it has no
place in `npm_partitions`' host dimension, and its own access log did not exist
before 2026-09-14 -- a different start date than NPM's, for a reason with no NPM
equivalent. `camera_zones_raw` is the precedent for this split: a snapshot whose own
start date cannot honestly share the events job's partitions gets its own asset and
its own job, rather than stretching one partitions definition to fit two things that
started on different days.
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

from homelab_datalake import layout
from homelab_datalake.geo import GeoEnricher
from homelab_datalake.parsing.extract import (
    ExtractResult,
    discover_logs,
    extract_mcp_api_partition,
    extract_partition,
    extract_partition_from_lake,
    reconciles,
)
from homelab_datalake.resources import LakeStorage, NpmLogSources

# 27 July 2026 is the oldest line the servers still hold. NPM rotates weekly and
# keeps four generations, so there is roughly a five-week window and no more --
# backfilling past this start date would produce empty partitions, not errors,
# which is worse. See IMPLEMENTATION_PLAN.md section 6.
LOG_RETENTION_START = "2026-07-27"

# Which NPM instances this deployment ingests, from NPM_HOSTS (comma-separated).
# An instance only belongs here once its source is actually configured -- a mounted
# directory for primary, the lake inbox for secondary -- and listing one that is not
# produces a partition that fails every night and tells you nothing you did not
# already know. secondary joined the default on 2026-09-14, once infra/secondary-log-sync/
# gave it a source at all; see the module docstring.
HOSTS = [h.strip() for h in os.environ.get("NPM_HOSTS", "primary,secondary").split(",") if h.strip()]

npm_partitions = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date=LOG_RETENTION_START),
        "host": StaticPartitionsDefinition(HOSTS),
    }
)

# Where secondary's NPM logs are mirrored to, hourly, by infra/secondary-log-sync/.
# A mirror of its log *directory*, not a transform of it -- same filenames, same
# rotation, `.gz` included -- which is what lets `extract_partition_from_lake` reuse
# the exact glob/format logic the primary path uses.
SECONDARY_INBOX_PREFIX = "inbox/npm/secondary"

# mcp.ai.example.test is not an NPM instance -- see the module docstring -- so it is
# not in HOSTS/npm_partitions, and its host_source value is not a NPM_HOSTS entry.
MCP_API_HOST = "mcp-api"

# The day the platform-side access log first exists is the platform team's
# decision, not NPM's -- overridable so the orchestrator can correct it without a
# code change if it ever moves again. 2026-09-15 is the day ai-platform's own
# compose actually started writing it (one day after the 2026-09-14 decision to
# build it at all -- see PLAN.md and privacy.py). A fixed date rather than one
# derived from the earliest file present, matching camera_zones_raw's precedent:
# simple, and correct as long as this constant is updated if the log ever moves.
MCP_API_LOG_START = os.environ.get("MCP_API_LOG_START", "2026-09-15")
mcp_api_partitions = DailyPartitionsDefinition(start_date=MCP_API_LOG_START)

RAW_PREFIX = layout.prefix("npm_access")
REJECT_PREFIX = layout.prefix("npm_access_rejects")

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
        # Names, never values: `strip_query` still removes the query string itself
        # from `path`. The raw value now lives in `query_string` below -- see the
        # 2026-09-14 decision in homelab_datalake.privacy.
        ("has_query", pa.bool_()),
        ("query_param_names", pa.list_(pa.string())),
        ("query_string", pa.string()),
        ("status", pa.int32()),
        ("upstream_status", pa.string()),
        ("cache_status", pa.string()),
        ("bytes_sent", pa.int64()),
        ("user_agent", pa.string()),
        ("referer", pa.string()),
        ("gzip_ratio", pa.string()),
        ("sent_to", pa.string()),
        # Raw, alongside its redacted counterparts below -- see the 2026-09-14
        # decision in homelab_datalake.privacy. Null for every row landed before
        # that date and for any row whose source line had already rotated off disk
        # when this shipped; see mcp.semantic.WEB_TRAFFIC for the exact boundary.
        ("client_address", pa.string()),
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


def _require_salt() -> str:
    salt = os.environ.get("CLIENT_IP_HASH_SALT", "")
    if not salt:
        # Failing here is deliberate. Defaulting to an empty salt would write
        # reversible hashes of real addresses, which is the exact outcome the
        # privacy boundary exists to prevent -- and it would do it silently.
        raise ValueError(
            "CLIENT_IP_HASH_SALT is not set. Refusing to run rather than write "
            "reversible client hashes. Generate one with: openssl rand -hex 32"
        )
    return salt


CHECK_REJECT_RATE = "reject_rate_within_threshold"
CHECK_NOT_EMPTY = "partition_is_not_silently_empty"
CHECK_RECONCILED = "lines_reconcile_to_rows_and_rejects"

_CHECK_SPECS = [
    AssetCheckSpec(
        name=CHECK_REJECT_RATE,
        asset="npm_access_raw",
        description=(
            "Fails when too many lines could not be parsed. A rising reject rate "
            "means the source changed its log format and the pipeline is silently "
            "losing traffic -- the failure a green dashboard would hide."
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
    AssetCheckSpec(
        name=CHECK_RECONCILED,
        asset="npm_access_raw",
        description=(
            "Fails when the lines read for this host and day do not equal the rows "
            "written plus the rejects written plus the rejects that duplicated one "
            "already written plus the lines that belonged to a different day. Every "
            "line seen has exactly one honest fate; if the count stops adding up, "
            "something silently dropped a line instead of accounting for it."
        ),
    ),
]


def _land_and_check(
    context: AssetExecutionContext, lake: LakeStorage, host: str, day: date, result: ExtractResult
):
    """Write one host-day partition's rows and rejects, then yield the Output and checks.

    Shared by `npm_access_raw` (primary, secondary) and `mcp_api_access_raw` so the three
    checks mean exactly the same thing regardless of which asset produced the
    partition, and so a change to what gets logged or checked cannot land in one
    and not the other.
    """
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

    yield AssetCheckResult(
        check_name=CHECK_RECONCILED,
        passed=reconciles(result),
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "lines_read": result.stats.total,
            "rows_written": len(result.rows),
            "rejects_written": len(result.rejects),
            "duplicate_rejects": result.duplicate_rejects,
            "lines_outside_partition": result.out_of_partition,
        },
    )


@asset(
    partitions_def=npm_partitions,
    group_name="npm",
    compute_kind="python",
    # Declared here rather than as standalone @asset_check functions. A standalone
    # check takes the asset's value as an input, which for a partitioned asset means
    # loading every partition back through the IO manager just to re-derive numbers
    # this run already has. Yielding the results inline keeps each one scoped to the
    # partition that produced it.
    check_specs=_CHECK_SPECS,
    # Reading a mounted file (or a broken lake mirror) should fail loudly: neither
    # fixes itself, and retrying only hides it. The retries here cover the
    # object-store write.
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description=(
        "One day of one host's NPM access logs, parsed, geo-enriched and redacted, "
        "landed as a single Parquet object. primary reads a mounted directory; secondary "
        "reads the hourly mirror at inbox/npm/secondary/ in the lake, because "
        "secondary cannot be mounted into this host. Client addresses are "
        "hashed as before, and -- since 2026-09-14 -- also kept raw alongside the "
        "hash; see homelab_datalake.privacy."
    ),
)
def npm_access_raw(
    context: AssetExecutionContext,
    lake: LakeStorage,
    npm_logs: NpmLogSources,
):
    day, host = partition_keys(context)
    salt = _require_salt()
    geo = GeoEnricher.from_env()

    if host == "secondary":
        # An inbox with nothing under it at all is a broken mirror, not a quiet
        # day -- the same distinction the mounted-directory branch below draws with
        # discover_logs, and for the same reason: a broken sync would otherwise
        # succeed silently with zero rows, forever, and look identical to secondary
        # genuinely having nothing to say that day.
        if not lake.list_keys(SECONDARY_INBOX_PREFIX):
            raise FileNotFoundError(
                f"no objects under {SECONDARY_INBOX_PREFIX!r} in the lake. This is a broken "
                "mirror, not an empty day -- check that infra/secondary-log-sync/'s cron job "
                "is running on secondary and that its key still has PutObject on "
                "this prefix. Refusing to write an empty partition that would look "
                "like a quiet one."
            )
        result = extract_partition_from_lake(lake, SECONDARY_INBOX_PREFIX, day, host, salt, geo)
    else:
        log_dir = npm_logs.directory_for(host)
        if not os.path.isdir(log_dir):
            raise FileNotFoundError(
                f"log directory {log_dir!r} for host {host!r} is not readable. "
                "A read-only bind mount of that instance's NPM /data/logs."
            )

        # An empty log directory is a configuration fault, not a quiet day, and the two
        # must not look alike. If a mount breaks, this directory silently empties and every
        # partition after it would succeed with zero rows. A run that is green and wrong is
        # worse than a run that fails.
        if not discover_logs(log_dir):
            raise FileNotFoundError(
                f"no access logs found in {log_dir!r} for host {host!r}. This is a broken "
                "mount, not an empty day -- refusing to write an empty partition that would "
                "look like one. Check the bind mount, and that NPM_HOSTS names only hosts "
                "whose source is actually configured."
            )
        result = extract_partition(log_dir, day, host, salt, geo)

    yield from _land_and_check(context, lake, host, day, result)


@asset(
    partitions_def=mcp_api_partitions,
    group_name="npm",
    compute_kind="python",
    check_specs=[
        AssetCheckSpec(name=CHECK_REJECT_RATE, asset="mcp_api_access_raw",
                        description=_CHECK_SPECS[0].description),
        AssetCheckSpec(name=CHECK_NOT_EMPTY, asset="mcp_api_access_raw",
                        description=_CHECK_SPECS[1].description),
        AssetCheckSpec(name=CHECK_RECONCILED, asset="mcp_api_access_raw",
                        description=_CHECK_SPECS[2].description),
    ],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    description=(
        "One day of mcp.ai.example.test's own JSON access log (see "
        "parsing.mcp_api_log), landed in the same raw.npm_access table as "
        "npm_access_raw under host_source='mcp-api'. Its own asset and its own "
        "partitions, starting the day the platform-side log first exists, because "
        "that has nothing to do with NPM's five-week retention window -- the same "
        "reason camera_zones_raw is not part of camera_daily_job."
    ),
)
def mcp_api_access_raw(
    context: AssetExecutionContext,
    lake: LakeStorage,
    npm_logs: NpmLogSources,
):
    day = date.fromisoformat(context.partition_key)
    salt = _require_salt()
    geo = GeoEnricher.from_env()

    log_dir = npm_logs.directory_for(MCP_API_HOST)
    if not os.path.isdir(log_dir):
        raise FileNotFoundError(
            f"log directory {log_dir!r} for host {MCP_API_HOST!r} is not readable. "
            "A read-only mount of the platform's mcp.ai access-log directory -- see "
            "MCP_API_LOG_DIR in docker-compose.yml."
        )
    # Unlike primary/secondary, "the directory holds no access-*.log file at all" is not
    # a broken-mount signal for this source and is not checked here. NPM's hosts
    # have been logging continuously for years, so an empty directory only ever
    # means a bad mount; this one started at MCP_API_LOG_START, with nothing
    # written before it and nothing guaranteed written the moment it starts
    # either -- the file for a given day may not exist until the first request of
    # that day lands. isdir is the one mount-health signal this source can
    # honestly give; extract_mcp_api_partition already treats a missing day file
    # as zero rows, not an error, and the checks below pass cleanly on zero rows
    # read from zero lines.
    result = extract_mcp_api_partition(log_dir, day, salt, geo)
    yield from _land_and_check(context, lake, MCP_API_HOST, day, result)
