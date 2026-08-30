"""Camera event assets.

Partitioned by date only -- one camera platform, reached over its public URL.

Unlike the other sources, a partition here is not final when it is first written.
Vision analysis lands hours after the event, so re-materialising an old day is a normal
thing to do rather than a repair. The asset is built for that: every row carries
`_extracted_at`, and `int_camera_events` keeps the newest version per event id.
"""

from datetime import date

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

from homelab_datalake.parsing.camera import extract_day
from homelab_datalake.resources import LakeStorage

CAMERA_START = "2026-07-01"

# end_offset=1 makes *today* a valid partition. The log assets deliberately do not do
# this, because a log day must be over before it is complete. A camera day is different:
# `camera_late_arrival_schedule` re-reads today every hour, because vision analysis
# lands hours after the event. Without the offset that schedule asks for a partition
# that does not exist yet, and Dagster raises DagsterUnknownPartitionError. The tick
# then dies before it can also request yesterday, so the whole late-arrival path stops.
camera_partitions = DailyPartitionsDefinition(start_date=CAMERA_START, end_offset=1)

EVENT_PREFIX = "raw/camera_events"
CLIP_PREFIX = "raw/camera_clips"
LOG_PREFIX = "raw/camera_logs"

# An event whose analysis never completed stays `candidate` forever. A few are normal --
# the day's last events are still in flight when the partition is first read -- but a
# high share means the analysis pipeline is stuck, and nothing else here would say so.
MAX_CANDIDATE_SHARE = 0.10

_EVENT_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("zoneId", pa.string()),
        ("zoneName", pa.string()),
        ("ruleId", pa.string()),
        ("ruleType", pa.string()),
        ("lifecycle", pa.string()),
        ("startedAt", pa.string()),
        ("completedAt", pa.string()),
        ("clipId", pa.string()),
        ("direction", pa.string()),
        ("state", pa.string()),
        ("previousState", pa.string()),
        ("stateObservedAt", pa.string()),
        ("confidence", pa.float64()),
        ("unusual", pa.bool_()),
        ("shouldNotify", pa.bool_()),
        ("dismissalReason", pa.string()),
        ("duplicateOf", pa.string()),
        ("summary", pa.string()),
    ]
)

_CLIP_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("kind", pa.string()),
        ("createdAt", pa.string()),
        ("savedAt", pa.string()),
        ("durationSeconds", pa.int64()),
        ("sizeBytes", pa.int64()),
        ("segmentCount", pa.int64()),
        ("storageQuality", pa.string()),
        ("archived", pa.bool_()),
        ("deleted", pa.bool_()),
        ("aiSource", pa.string()),
        ("aiType", pa.string()),
        ("localConfidence", pa.float64()),
        ("createdByHash", pa.string()),
        ("annotationCount", pa.int64()),
    ]
)

_LOG_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("type", pa.string()),
        ("important", pa.bool_()),
        ("createdAt", pa.string()),
        ("sourceId", pa.string()),
        ("userHash", pa.string()),
    ]
)

_SCHEMAS = {"events": _EVENT_SCHEMA, "clips": _CLIP_SCHEMA, "logs": _LOG_SCHEMA}
_PREFIXES = {"events": EVENT_PREFIX, "clips": CLIP_PREFIX, "logs": LOG_PREFIX}

CHECK_ANALYSIS_COMPLETES = "vision_analysis_is_completing"


def _project(rows: list[dict], schema: pa.Schema) -> list[dict]:
    """Keep exactly the declared columns.

    A field the export starts sending is ignored until it is added here, which is what
    stops an upstream change from silently widening the warehouse -- including one that
    adds something nobody meant to store.
    """
    names = [f.name for f in schema]
    return [{name: row.get(name) for name in names} for row in rows]


@asset(
    partitions_def=camera_partitions,
    group_name="camera",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_ANALYSIS_COMPLETES,
            asset="camera_events_raw",
            description=(
                "Warns when too many events are still `candidate`. A few is normal -- "
                "the day's last events are in flight when the partition is first read "
                "-- but a high share means vision analysis is stuck, and no row count "
                "would show that."
            ),
        )
    ],
    description=(
        "One day of camera events, clips and activity logs, read over HTTPS with a "
        "Keycloak service-account token. No zone geometry, no media, hashed identities."
    ),
)
def camera_events_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)

    result = extract_day(day)
    written = 0
    for kind in ("events", "clips", "logs"):
        prefix = f"{_PREFIXES[kind]}/dt={day.isoformat()}"
        lake.delete_prefix(prefix)
        table = pa.Table.from_pylist(_project(result.rows(kind), _SCHEMAS[kind]), schema=_SCHEMAS[kind])
        written += lake.write_table(f"{prefix}/data.parquet", table)

    events = result.events
    candidates = sum(1 for e in events if e.get("lifecycle") == "candidate")
    candidate_share = candidates / len(events) if events else 0.0
    confirmed = sum(1 for e in events if e.get("lifecycle") == "confirmed")

    context.log.info(
        "dt=%s %d events (%d confirmed, %d still candidate), %d clips, %d logs",
        day, len(events), confirmed, candidates, len(result.clips), len(result.logs),
    )

    yield Output(
        {"events": len(events), "clips": len(result.clips), "logs": len(result.logs)},
        metadata={
            "events": len(events),
            "clips": len(result.clips),
            "logs": len(result.logs),
            "confirmed": confirmed,
            "still_candidate": candidates,
            "cameras": len({e.get("cameraId") for e in events if e.get("cameraId")}),
            "bytes_written": written,
            "extracted_at": MetadataValue.text(str(result.extracted_at)),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_ANALYSIS_COMPLETES,
        passed=candidate_share <= MAX_CANDIDATE_SHARE,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "candidate_share": MetadataValue.float(round(candidate_share, 4)),
            "threshold": MAX_CANDIDATE_SHARE,
            "events": len(events),
        },
    )
