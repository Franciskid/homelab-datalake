"""Camera assets.

Partitioned by date only -- one camera platform, reached over its public URL.

Unlike the other sources, a partition here is not final when it is first written.
Vision analysis lands hours after the event, so re-materialising an old day is a normal
thing to do rather than a repair.

Two properties make that safe, and both are load-bearing.

**A partition is merged, never replaced.** This asset used to delete the prefix and
write whatever the export returned. That was correct while the camera still held the
day, and it stopped being correct: the camera's event store is capped at [count withheld] events
and evicts the oldest, so a re-read of an old day legitimately returns *fewer* rows than
the lake already holds. Measured on 2026-08-31, partition 2026-08-08 held [count withheld] events in
the lake and the export returned 287 -- a replace would have destroyed [count withheld] events that
exist nowhere else. So the lake is the accumulator: it keeps every version it has ever
seen and takes the newest per event id.

**Nothing the source sends is discarded silently.** Every schema below is the full
field set the export offers, and anything it starts sending that is *not* named here
lands in `_unmapped` as JSON instead of vanishing. The old projection was
deny-by-default with no signal, which is how `counts` -- the subject breakdown every
graph of this data is built from -- was dropped on every row since the first
partition, with nothing anywhere saying so. Deny-by-default is still right; silence
was not.
"""

from datetime import date, datetime

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

from homelab_datalake import layout
from homelab_datalake.parsing.camera import extract_day, extract_rows
from homelab_datalake.projection import project
from homelab_datalake.resources import LakeStorage

CAMERA_START = "2026-07-01"

# end_offset=1 makes *today* a valid partition. The log assets deliberately do not do
# this, because a log day must be over before it is complete. A camera day is different:
# `camera_late_arrival_schedule` re-reads today every hour, because vision analysis
# lands hours after the event. Without the offset that schedule asks for a partition
# that does not exist yet, and Dagster raises DagsterUnknownPartitionError. The tick
# then dies before it can also request yesterday, so the whole late-arrival path stops.
camera_partitions = DailyPartitionsDefinition(start_date=CAMERA_START, end_offset=1)

# Built from the registry, never spelled out here: layout.py is the one place a
# path is decided, so moving a dataset is one edit and the migration can see it.
EVENT_PREFIX = layout.prefix("camera_events")
CLIP_PREFIX = layout.prefix("camera_clips")
LOG_PREFIX = layout.prefix("camera_logs")
ZONE_PREFIX = layout.prefix("camera_zones")
OBSERVABILITY_PREFIX = layout.prefix("camera_observability")
ANNOTATION_PREFIX = layout.prefix("camera_annotations")

# An event whose analysis never completed stays `candidate` forever. A few are normal --
# the day's last events are still in flight when the partition is first read -- but a
# high share means the analysis pipeline is stuck, and nothing else here would say so.
MAX_CANDIDATE_SHARE = 0.10

# The subject breakdown the camera counts, and the one the alerts graph sums over.
# A struct rather than four flat columns because that is the shape the source sends
# and the shape a reader wants: DuckDB reads Parquet structs natively, so
# `counts.person` works in plain SQL with nothing to unpack first.
_COUNTS = pa.struct(
    [
        ("person", pa.int64()),
        ("vehicle", pa.int64()),
        ("animal", pa.int64()),
        ("other", pa.int64()),
    ]
)

# Exactly the camera's own CORRECTABLE_FIELDS: everything a person may overwrite on an
# event, and therefore everything worth keeping the machine's original answer for.
_CORRECTION_PREVIOUS = pa.struct(
    [
        ("lifecycle", pa.string()),
        ("direction", pa.string()),
        ("counts", _COUNTS),
        ("state", pa.string()),
        ("previousState", pa.string()),
        ("stateObservedAt", pa.string()),
        ("dismissalReason", pa.string()),
        ("duplicateOf", pa.string()),
        ("summary", pa.string()),
        ("confidence", pa.float64()),
        ("shouldNotify", pa.bool_()),
    ]
)

# The raw layer keeps *both* accounts: the corrected values sit on the row itself, and
# what the machine originally said sits here beside them. Raw is evidence; the mart is
# where one of the two is chosen as the truth.
_CORRECTION = pa.struct(
    [
        ("correctedAt", pa.string()),
        # A stable pseudonym, hashed at the camera with its own salt. The name travels
        # because "who overrode this" is the question, and it is answered by a name.
        ("correctedByHash", pa.string()),
        ("correctedByName", pa.string()),
        ("previous", _CORRECTION_PREVIOUS),
    ]
)

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
        # The camera's own report ranks and filters on `occurredAt || startedAt`. The
        # lake only ever saw `startedAt`, so it could not reproduce its own source's
        # ordering.
        ("occurredAt", pa.string()),
        ("completedAt", pa.string()),
        ("clipId", pa.string()),
        # The clip that filmed the gate actually moving. `clipId` for a state change
        # shows a gate that has already finished moving, so a reader wanting the
        # transition wants this one.
        ("transitionClipId", pa.string()),
        ("direction", pa.string()),
        ("state", pa.string()),
        ("previousState", pa.string()),
        ("stateObservedAt", pa.string()),
        ("stateSource", pa.string()),
        ("counts", _COUNTS),
        ("confidence", pa.float64()),
        ("unusual", pa.bool_()),
        ("shouldNotify", pa.bool_()),
        ("dismissed", pa.bool_()),
        ("dismissalReason", pa.string()),
        ("duplicateOf", pa.string()),
        ("summary", pa.string()),
        ("correction", _CORRECTION),
        # Rare provenance blocks -- single figures on a handful of rows each. JSON
        # rather than five more struct columns: DuckDB reads them with json_extract,
        # and a column that is null on [count withheld] of [count withheld] rows does not earn a schema.
        ("backfill", pa.string()),
        ("repair", pa.string()),
        ("restoration", pa.string()),
        ("timingCorrection", pa.string()),
        ("_unmapped", pa.string()),
    ]
)

_CLIP_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("kind", pa.string()),
        # What the clip was labelled -- "1 personne". Sent on every clip, kept on none
        # of them until now.
        ("title", pa.string()),
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
        ("_unmapped", pa.string()),
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
        # Free text people wrote, @mentions and all. Held back originally for that
        # reason; landed now because these are the household's own notes in their own
        # installation, and the owner asked for the whole record.
        ("title", pa.string()),
        ("userHash", pa.string()),
        ("_unmapped", pa.string()),
    ]
)

# One rule per row rather than a list on the zone: a rule is what an event points at
# through `ruleId`, so it needs to be joinable, and a nested list is not.
_ZONE_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("name", pa.string()),
        ("enabled", pa.bool_()),
        ("revision", pa.int64()),
        ("geometryRevision", pa.int64()),
        ("description", pa.string()),
        ("scope", pa.string()),
        # Counts, not contents: "is this zone calibrated" is answerable, "where is the
        # gate in the frame" is not.
        ("anchorCount", pa.int64()),
        ("stateReferenceCount", pa.int64()),
        ("revisionCount", pa.int64()),
        ("feedbackCount", pa.int64()),
        ("autoLearnAnchors", pa.bool_()),
        ("maxAnchors", pa.int64()),
        ("createdByHash", pa.string()),
        ("createdByName", pa.string()),
        ("updatedByHash", pa.string()),
        ("updatedByName", pa.string()),
        ("ruleId", pa.string()),
        ("ruleType", pa.string()),
        ("ruleName", pa.string()),
        ("ruleEnabled", pa.bool_()),
        ("ruleNotify", pa.bool_()),
        ("ruleCooldownSec", pa.int64()),
        ("ruleSensitivity", pa.string()),
        ("rulePrompt", pa.string()),
        ("ruleTargets", pa.list_(pa.string())),
        ("ruleStates", pa.list_(pa.string())),
        ("ruleAlertOn", pa.list_(pa.string())),
        ("createdAt", pa.string()),
        ("updatedAt", pa.string()),
        # The geometry and the history behind it, as JSON. This repo argued for holding
        # the polygons back -- a map of the property is not analytics -- and the lake's
        # owner decided otherwise for their own installation. Lossless as JSON, with
        # the counts above kept because most questions want a count, not a polygon.
        ("shapesJson", pa.string()),
        ("ignoreShapesJson", pa.string()),
        ("anchorsJson", pa.string()),
        ("stateReferencesJson", pa.string()),
        ("anchorPolicyJson", pa.string()),
        ("revisionsJson", pa.string()),
        ("feedbackJson", pa.string()),
        ("_unmapped", pa.string()),
    ]
)

_OBSERVABILITY_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("cameraId", pa.string()),
        ("zoneId", pa.string()),
        ("reason", pa.string()),
        ("detail", pa.string()),
        ("fromAt", pa.string()),
        ("toAt", pa.string()),
        ("ongoing", pa.bool_()),
        ("_unmapped", pa.string()),
    ]
)

_ANNOTATION_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.string()),
        ("scope", pa.string()),
        ("targetId", pa.string()),
        ("kind", pa.string()),
        ("emoji", pa.string()),
        ("offsetSec", pa.float64()),
        ("timestampMs", pa.int64()),
        ("createdAt", pa.string()),
        # The note itself, for the same reason the log title travels.
        ("text", pa.string()),
        ("userHash", pa.string()),
        ("zoneId", pa.string()),
        ("ruleId", pa.string()),
        ("eventType", pa.string()),
        ("_unmapped", pa.string()),
    ]
)

_SCHEMAS = {"events": _EVENT_SCHEMA, "clips": _CLIP_SCHEMA, "logs": _LOG_SCHEMA}
_PREFIXES = {"events": EVENT_PREFIX, "clips": CLIP_PREFIX, "logs": LOG_PREFIX}

# Columns whose value arrives as an object and is stored as JSON text.
_JSON_COLUMNS = {"backfill", "repair", "restoration", "timingCorrection"}

CHECK_ANALYSIS_COMPLETES = "vision_analysis_is_completing"
CHECK_NO_UNMAPPED = "no_unmapped_fields"
CHECK_NOT_SHRUNK = "partition_did_not_shrink"


# --------------------------------------------------------------------------------
# projection: keep everything, name what is known, capture what is not
# --------------------------------------------------------------------------------


def _project(rows: list[dict], schema: pa.Schema) -> tuple[list[dict], set[str]]:
    """The shared projection, with this source's JSON-valued columns named."""
    return project(rows, schema, json_columns=_JSON_COLUMNS)


# --------------------------------------------------------------------------------
# merging: the lake accumulates, the source only offers
# --------------------------------------------------------------------------------


def _extracted_at(row: dict) -> datetime | None:
    value = row.get("_extracted_at")
    return value if isinstance(value, datetime) else None


def _supersedes(candidate: dict, incumbent: dict) -> bool:
    """Whether `candidate` is the version of an event to keep.

    The same rule `int_camera_events` applies downstream, moved up to the write so
    the raw layer is already one row per id per partition: newest `_extracted_at`
    wins, and `lifecycle <> 'candidate'` breaks a tie, because an analysed event is
    never superseded by an unanalysed one. Ordering by the timestamp alone is not
    enough -- two versions can share a microsecond when a re-read is fast.
    """
    new_at, old_at = _extracted_at(candidate), _extracted_at(incumbent)
    if new_at is not None and old_at is not None and new_at != old_at:
        return new_at > old_at
    return (candidate.get("lifecycle") != "candidate") and (
        incumbent.get("lifecycle") == "candidate"
    )


def _merge(existing: list[dict], incoming: list[dict], schema: pa.Schema) -> tuple[list[dict], int]:
    """Union two versions of a partition on `id`, newest per id.

    Returns the merged rows and how many of them the source no longer offers -- rows
    the lake is now the only copy of. That number is the whole reason this function
    exists, and it is reported as metadata so it is visible rather than inferred.
    """
    names = [f.name for f in schema]
    incoming_ids = {row.get("id") for row in incoming}
    merged: dict[str, dict] = {}

    for row in list(existing) + list(incoming):
        # Normalise to the current schema: rows read back from an older partition are
        # missing every column added since they were written.
        normalised = {name: row.get(name) for name in names}
        key = normalised.get("id")
        if key is None:
            continue
        incumbent = merged.get(key)
        if incumbent is None or _supersedes(normalised, incumbent):
            merged[key] = normalised

    retained_only_in_lake = sum(1 for key in merged if key not in incoming_ids)
    return list(merged.values()), retained_only_in_lake


def _read_partition(lake: LakeStorage, key: str, schema: pa.Schema) -> list[dict]:
    table = lake.read_table(key)
    return table.to_pylist() if table is not None else []


def _write_merged(
    lake: LakeStorage, prefix: str, rows: list[dict], schema: pa.Schema
) -> tuple[int, int, int, int]:
    """Merge `rows` into the partition at `prefix` and write it back.

    Returns (bytes written, rows after, rows before, rows only the lake still has).
    """
    key = f"{prefix}/data.parquet"
    existing = _read_partition(lake, key, schema)
    merged, retained = _merge(existing, rows, schema)

    # Delete the prefix, then write one object. The delete is still here because a
    # partition written before the layout change may hold several objects under this
    # prefix -- but it happens *after* the read, so nothing is lost between the two.
    lake.delete_prefix(prefix)
    table = pa.Table.from_pylist(merged, schema=schema)
    written = lake.write_table(key, table)
    return written, len(merged), len(existing), retained


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
        ),
        AssetCheckSpec(
            name=CHECK_NOT_SHRUNK,
            asset="camera_events_raw",
            description=(
                "Fails when a re-materialised partition holds fewer events than it did "
                "before. The merge makes that impossible by construction, so this "
                "firing means the merge itself is broken -- and the failure it guards "
                "against is silent and permanent: the camera evicts old events, so a "
                "partition that shrinks has lost the only remaining copy."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_NO_UNMAPPED,
            asset="camera_events_raw",
            description=(
                "Warns when the export sent a field the schema does not name. The "
                "value is kept in `_unmapped` rather than dropped, so this is not data "
                "loss -- it is the signal whose absence let `counts` go missing on "
                "every row for a month."
            ),
        ),
    ],
    description=(
        "One day of camera events, clips and activity logs, read over HTTPS with a "
        "Keycloak service-account token. Merged into the partition rather than "
        "replacing it. No zone geometry, no media, hashed identities."
    ),
)
def camera_events_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)

    result = extract_day(day)
    written = 0
    totals: dict[str, dict] = {}
    unmapped_all: set[str] = set()

    for kind in ("events", "clips", "logs"):
        schema = _SCHEMAS[kind]
        prefix = f"{_PREFIXES[kind]}/dt={day.isoformat()}"
        rows, unmapped = _project(result.rows(kind), schema)
        unmapped_all.update(f"{kind}.{name}" for name in unmapped)
        size, after, before, retained = _write_merged(lake, prefix, rows, schema)
        written += size
        totals[kind] = {
            "offered": len(rows), "after": after, "before": before, "retained": retained
        }

    events = result.events
    candidates = sum(1 for e in events if e.get("lifecycle") == "candidate")
    candidate_share = candidates / len(events) if events else 0.0
    confirmed = sum(1 for e in events if e.get("lifecycle") == "confirmed")
    event_totals = totals["events"]

    context.log.info(
        "dt=%s events: %d offered, %d in partition (was %d, %d kept that the source no "
        "longer has); %d confirmed, %d still candidate; clips %d, logs %d",
        day, event_totals["offered"], event_totals["after"], event_totals["before"],
        event_totals["retained"], confirmed, candidates,
        totals["clips"]["after"], totals["logs"]["after"],
    )
    if unmapped_all:
        context.log.warning(
            "dt=%s the export sent fields the schema does not name, kept in _unmapped: %s",
            day, ", ".join(sorted(unmapped_all)),
        )

    yield Output(
        {
            "events": event_totals["after"],
            "clips": totals["clips"]["after"],
            "logs": totals["logs"]["after"],
        },
        metadata={
            "events": event_totals["after"],
            "events_offered_by_source": event_totals["offered"],
            "events_before_merge": event_totals["before"],
            # The number that says the merge is doing something: rows the camera has
            # already evicted and only the lake still holds.
            "events_only_in_lake": event_totals["retained"],
            "clips": totals["clips"]["after"],
            "logs": totals["logs"]["after"],
            "confirmed": confirmed,
            "still_candidate": candidates,
            "cameras": len({e.get("cameraId") for e in events if e.get("cameraId")}),
            "bytes_written": written,
            "unmapped_fields": MetadataValue.text(", ".join(sorted(unmapped_all)) or "none"),
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

    shrunk = [k for k, v in totals.items() if v["after"] < v["before"]]
    yield AssetCheckResult(
        check_name=CHECK_NOT_SHRUNK,
        passed=not shrunk,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "shrunk": MetadataValue.text(", ".join(shrunk) or "none"),
            **{f"{k}_before": v["before"] for k, v in totals.items()},
            **{f"{k}_after": v["after"] for k, v in totals.items()},
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_NO_UNMAPPED,
        passed=not unmapped_all,
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(unmapped_all)) or "none")},
    )


# --------------------------------------------------------------------------------
# the reference and coverage exports
# --------------------------------------------------------------------------------
#
# Three assets rather than three more objects inside `camera_events_raw`, because
# they fail independently and mean different things. A zone export being down is not
# a reason for a day of events not to land.

# Zone configuration has no history at the camera: asking for it returns how the zones
# are set up *now*. So a partition before collection began cannot be filled, for the
# same reason `jellyfin_library_raw` refuses to -- writing today's configuration under
# an old date would invent a past, and every event joined to it would then be described
# by rules that did not exist when it fired. Collection starts here.
ZONE_START = "2026-08-31"
zone_partitions = DailyPartitionsDefinition(start_date=ZONE_START, end_offset=1)

# A zone list does not lose a fifth of itself overnight; that is a half-written config
# or a partial read, and recording it would make every event on the day look as though
# it fired against a rule nobody had configured.
MAX_ZONE_SHRINK_RATIO = 0.20

CHECK_ZONES_NOT_SHRUNK = "zone_list_has_not_shrunk_unexpectedly"


def _reference_output(
    context: AssetExecutionContext,
    lake: LakeStorage,
    day: date,
    endpoint: str,
    prefix: str,
    schema: pa.Schema,
    merge: bool,
):
    """Land one day of one reference endpoint. Shared by the three assets below."""
    raw_rows, extracted_at = extract_rows(day, endpoint)
    rows, unmapped = _project(raw_rows, schema)
    partition_prefix = f"{prefix}/dt={day.isoformat()}"

    if merge:
        written, after, before, retained = _write_merged(lake, partition_prefix, rows, schema)
    else:
        before = len(_read_partition(lake, f"{partition_prefix}/data.parquet", schema))
        lake.delete_prefix(partition_prefix)
        written = lake.write_table(
            f"{partition_prefix}/data.parquet", pa.Table.from_pylist(rows, schema=schema)
        )
        after, retained = len(rows), 0

    context.log.info(
        "dt=%s %s: %d offered, %d in partition (was %d)%s",
        day, endpoint, len(rows), after, before,
        f", {retained} kept that the source no longer has" if retained else "",
    )
    if unmapped:
        context.log.warning(
            "dt=%s %s sent fields the schema does not name, kept in _unmapped: %s",
            day, endpoint, ", ".join(sorted(unmapped)),
        )
    return {
        "rows": after, "before": before, "offered": len(rows), "retained": retained,
        "written": written, "unmapped": unmapped, "extracted_at": extracted_at,
    }


@asset(
    partitions_def=zone_partitions,
    group_name="camera",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_ZONES_NOT_SHRUNK,
            asset="camera_zones_raw",
            description=(
                "Fails when the zone list shrinks by more than a fifth against the "
                "previous snapshot -- a partial read, not a reconfiguration."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_NO_UNMAPPED,
            asset="camera_zones_raw",
            description="Warns when the export sent a field the schema does not name.",
        ),
    ],
    description=(
        "Attention-zone configuration as it stands today, one row per zone and rule. "
        "Zone id, name, rule type and threshold; never the polygon geometry."
    ),
)
def camera_zones_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)
    # Replaced, not merged: this is a snapshot, and a zone deleted today should be
    # absent from today's snapshot rather than preserved out of caution.
    out = _reference_output(
        context, lake, day, "zones", ZONE_PREFIX, _ZONE_SCHEMA, merge=False
    )

    yield Output(
        {"zones": out["rows"]},
        metadata={
            "rows": out["rows"],
            "previous_rows": out["before"],
            "bytes_written": out["written"],
            "unmapped_fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none"),
            "extracted_at": MetadataValue.text(str(out["extracted_at"])),
        },
    )

    shrunk = out["before"] > 0 and out["rows"] < out["before"] * (1 - MAX_ZONE_SHRINK_RATIO)
    yield AssetCheckResult(
        check_name=CHECK_ZONES_NOT_SHRUNK,
        passed=not shrunk,
        severity=AssetCheckSeverity.ERROR,
        metadata={"rows": out["rows"], "previous": out["before"]},
    )
    yield AssetCheckResult(
        check_name=CHECK_NO_UNMAPPED,
        passed=not out["unmapped"],
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none")},
    )


@asset(
    partitions_def=camera_partitions,
    group_name="camera",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NO_UNMAPPED,
            asset="camera_observability_raw",
            description="Warns when the export sent a field the schema does not name.",
        ),
    ],
    description=(
        "Spans where a zone could not be read, and why. Without these a stretch the "
        "camera could not see is indistinguishable from a gate that stayed shut."
    ),
)
def camera_observability_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)
    out = _reference_output(
        context, lake, day, "observability", OBSERVABILITY_PREFIX,
        _OBSERVABILITY_SCHEMA, merge=True,
    )
    yield Output(
        {"spans": out["rows"]},
        metadata={
            "spans": out["rows"],
            "spans_only_in_lake": out["retained"],
            "bytes_written": out["written"],
            "unmapped_fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none"),
        },
    )
    yield AssetCheckResult(
        check_name=CHECK_NO_UNMAPPED,
        passed=not out["unmapped"],
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none")},
    )


@asset(
    partitions_def=camera_partitions,
    group_name="camera",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NO_UNMAPPED,
            asset="camera_annotations_raw",
            description="Warns when the export sent a field the schema does not name.",
        ),
    ],
    description=(
        "Notes and marks left against a clip or an event. The free text itself is "
        "never exported; what is kept is that a note exists, on what, and by whom."
    ),
)
def camera_annotations_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)
    out = _reference_output(
        context, lake, day, "annotations", ANNOTATION_PREFIX,
        _ANNOTATION_SCHEMA, merge=True,
    )
    yield Output(
        {"annotations": out["rows"]},
        metadata={
            "annotations": out["rows"],
            "annotations_only_in_lake": out["retained"],
            "bytes_written": out["written"],
            "unmapped_fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none"),
        },
    )
    yield AssetCheckResult(
        check_name=CHECK_NO_UNMAPPED,
        passed=not out["unmapped"],
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(out["unmapped"])) or "none")},
    )
