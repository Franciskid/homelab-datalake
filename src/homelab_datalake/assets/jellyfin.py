"""Jellyfin library assets.

A full snapshot per day, partitioned by date only -- one library, one server.

Two objects per partition rather than one: titles, and the title-to-genre pairs. A
title has many genres, so flattening them into the title row would mean either a
delimited string nobody can filter on or `genre_1, genre_2, genre_3`. The pair table is
the honest shape and becomes the bridge in the mart.

Three more assets live in this module since 2026-09-14: `jellyfin_playback_raw`,
`jellyfin_activity_raw` and `jellyfin_users_raw`. See `parsing.jellyfin` for why they
exist now and did not before. They read different endpoints on their own partitioning
-- hourly for playback, daily for the other two -- but they are the same source,
reached with the same `JELLYFIN_URL`/`JELLYFIN_API_KEY`, so they stay beside the
library assets rather than in a module of their own, the way every camera export
endpoint stays in one `assets/camera.py`.
"""

import os
from datetime import UTC, date, datetime

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    HourlyPartitionsDefinition,
    MetadataValue,
    Output,
    RetryPolicy,
    asset,
)

from homelab_datalake import layout
from homelab_datalake.parsing import jellyfin as jellyfin_source
from homelab_datalake.parsing.jellyfin import snapshot
from homelab_datalake.projection import project
from homelab_datalake.resources import LakeStorage

# There is no history before the pipeline: a snapshot is of the library *now*, so
# backfilling earlier dates would write today's library under yesterday's date and
# quietly invent a past. The start date is when collection began.
LIBRARY_START = "2026-08-28"

# end_offset=1 makes *today* a valid partition, which the log assets deliberately do
# not do. The difference is what the partition means: a log partition covers a day that
# has to be over before it is complete, while a snapshot describes the library at the
# moment it is taken. Dating today's snapshot yesterday would be recording a state that
# never existed on that date.
library_partitions = DailyPartitionsDefinition(start_date=LIBRARY_START, end_offset=1)

TITLE_PREFIX = layout.prefix("jellyfin_titles")
GENRE_PREFIX = layout.prefix("jellyfin_title_genres")
STREAM_PREFIX = layout.prefix("jellyfin_title_streams")
PEOPLE_PREFIX = layout.prefix("jellyfin_title_people")

# A library does not lose a fifth of itself overnight. A drop that large means a mount
# went missing or a scan is mid-flight, and writing that snapshot would record a
# fiction as history -- which SCD2 would then faithfully preserve.
MAX_SHRINK_RATIO = 0.20

_TITLE_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("item_id", pa.string()),
        ("title", pa.string()),
        ("original_title", pa.string()),
        ("production_year", pa.int32()),
        ("premiere_date", pa.date32()),
        ("added_at", pa.timestamp("us", tz="UTC")),
        ("community_rating", pa.float64()),
        ("critic_rating", pa.float64()),
        ("official_rating", pa.string()),
        ("runtime_minutes", pa.int32()),
        ("container", pa.string()),
        ("video_codec", pa.string()),
        ("video_height", pa.int32()),
        ("video_width", pa.int32()),
        ("video_bit_depth", pa.int32()),
        ("resolution_class", pa.string()),
        ("audio_codec", pa.string()),
        ("audio_channels", pa.int32()),
        ("tmdb_id", pa.string()),
        ("imdb_id", pa.string()),
        ("tmdb_collection_id", pa.string()),
        ("tvdb_id", pa.string()),
        # The primary studio and country stay, and the full lists travel beside them.
        # One title in this library has twenty-five studios and two countries.
        ("studio", pa.string()),
        ("studios", pa.list_(pa.string())),
        ("production_country", pa.string()),
        ("production_countries", pa.list_(pa.string())),
        ("decade", pa.int32()),
        ("sort_name", pa.string()),
        ("overview", pa.string()),
        ("tagline", pa.string()),
        ("taglines", pa.list_(pa.string())),
        ("tags", pa.list_(pa.string())),
        ("parent_id", pa.string()),
        ("item_type", pa.string()),
        ("media_type", pa.string()),
        ("video_type", pa.string()),
        ("has_subtitles", pa.bool_()),
        # Derived from the streams so the common question -- "what can I watch in
        # French" -- does not need the bridge table.
        ("subtitle_languages", pa.list_(pa.string())),
        ("audio_languages", pa.list_(pa.string())),
        ("stream_count", pa.int32()),
        ("chapter_count", pa.int32()),
        ("remote_trailer_count", pa.int32()),
        ("external_urls", pa.list_(pa.struct([("name", pa.string()), ("url", pa.string())]))),
        ("file_size_bytes", pa.int64()),
        ("file_bitrate", pa.int64()),
        ("media_source_count", pa.int32()),
        ("_unmapped", pa.string()),
    ]
)

_GENRE_SCHEMA = pa.schema(
    [("dt", pa.date32()), ("item_id", pa.string()), ("genre", pa.string())]
)

# One row per media stream. The whole reason this table exists is that keeping the
# first video and the first audio stream lost every subtitle track in the library.
_STREAM_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("item_id", pa.string()),
        ("stream_index", pa.int32()),
        ("stream_type", pa.string()),
        ("codec", pa.string()),
        ("language", pa.string()),
        ("title", pa.string()),
        ("display_title", pa.string()),
        ("is_default", pa.bool_()),
        ("is_forced", pa.bool_()),
        ("is_external", pa.bool_()),
        ("is_hearing_impaired", pa.bool_()),
        ("is_interlaced", pa.bool_()),
        ("height", pa.int32()),
        ("width", pa.int32()),
        ("channels", pa.int32()),
        ("channel_layout", pa.string()),
        ("bit_rate", pa.int64()),
        ("bit_depth", pa.int32()),
        ("sample_rate", pa.int32()),
        ("video_range", pa.string()),
        ("video_range_type", pa.string()),
        ("profile", pa.string()),
        ("level", pa.float64()),
    ]
)

# Cast and crew. Public film metadata: who acted in and made a film, not anything
# about anybody using this library.
_PEOPLE_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("item_id", pa.string()),
        ("person_id", pa.string()),
        ("name", pa.string()),
        ("person_type", pa.string()),
        ("role", pa.string()),
        ("sort_order", pa.int32()),
    ]
)

CHECK_NOT_SHRUNK = "library_has_not_shrunk_unexpectedly"
CHECK_TMDB_COVERAGE = "tmdb_id_coverage"
CHECK_NO_UNMAPPED = "no_unmapped_fields"


@asset(
    partitions_def=library_partitions,
    group_name="jellyfin",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_NOT_SHRUNK,
            asset="jellyfin_library_raw",
            description=(
                "Fails when the library shrinks by more than a fifth against the "
                "previous snapshot. That is a missing mount or a scan in flight, and "
                "SCD2 would preserve the fiction as history."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_TMDB_COVERAGE,
            asset="jellyfin_library_raw",
            description=(
                "Warns when TMDB id coverage drops below 95%. Enrichment is a key "
                "lookup only while the keys are there; below that it quietly degrades "
                "to matching on titles, which gets remakes wrong."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_NO_UNMAPPED,
            asset="jellyfin_library_raw",
            description=(
                "Warns when Jellyfin returned a field nothing here reads. The value is "
                "kept in `_unmapped` rather than dropped; this is the prompt to give it "
                "a column."
            ),
        ),
    ],
    description=(
        "A daily snapshot of the movie library: metadata, codecs and ratings. No file "
        "paths. Playback history is `jellyfin_playback_raw`, further down this module."
    ),
)
def jellyfin_library_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)

    result = snapshot(day)
    if not result.titles:
        raise ValueError(
            f"Jellyfin returned no titles for {day}. An empty library is a broken mount "
            "or an unreachable server, not a real state -- refusing to write it."
        )

    written = 0
    for prefix, rows, schema in (
        (TITLE_PREFIX, result.titles, _TITLE_SCHEMA),
        (GENRE_PREFIX, result.genres, _GENRE_SCHEMA),
        (STREAM_PREFIX, result.streams, _STREAM_SCHEMA),
        (PEOPLE_PREFIX, result.people, _PEOPLE_SCHEMA),
    ):
        partition = f"{prefix}/dt={day.isoformat()}"
        # Replaced, not merged: a snapshot describes the library at a moment, and a
        # title deleted from it should be absent rather than preserved. The shrink
        # check below is what makes that safe -- the camera's problem, where the
        # source forgets and the lake is the only copy, does not arise here.
        lake.delete_prefix(partition)
        projected, _ = project(rows, schema)
        written += lake.write_table(
            f"{partition}/data.parquet", pa.Table.from_pylist(projected, schema=schema)
        )

    with_tmdb = sum(1 for t in result.titles if t["tmdb_id"])
    coverage = with_tmdb / result.count
    previous = _previous_count(lake, day)

    context.log.info(
        "dt=%s %d titles (previous snapshot: %s), %.1f%% with a TMDB id",
        day, result.count, previous if previous is not None else "none", 100 * coverage,
    )

    yield Output(
        {"titles": result.count, "tmdb_coverage": coverage},
        metadata={
            "titles": result.count,
            "genre_pairs": len(result.genres),
            "distinct_genres": len({g["genre"] for g in result.genres}),
            "media_streams": len(result.streams),
            "credits": len(result.people),
            "tmdb_coverage_pct": MetadataValue.float(round(100 * coverage, 2)),
            "pages_fetched": result.pages,
            "bytes_written": written,
            "unmapped_fields": MetadataValue.text(", ".join(sorted(result.unmapped)) or "none"),
            "destination": MetadataValue.path(
                f"{lake.describe()}/{TITLE_PREFIX}/dt={day.isoformat()}/data.parquet"
            ),
        },
    )

    shrunk = previous is not None and result.count < previous * (1 - MAX_SHRINK_RATIO)
    yield AssetCheckResult(
        check_name=CHECK_NOT_SHRUNK,
        passed=not shrunk,
        severity=AssetCheckSeverity.ERROR,
        metadata={"titles": result.count, "previous": previous or 0},
    )

    yield AssetCheckResult(
        check_name=CHECK_TMDB_COVERAGE,
        passed=coverage >= 0.95,
        severity=AssetCheckSeverity.WARN,
        metadata={"coverage_pct": MetadataValue.float(round(100 * coverage, 2))},
    )

    yield AssetCheckResult(
        check_name=CHECK_NO_UNMAPPED,
        passed=not result.unmapped,
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(result.unmapped)) or "none")},
    )


def _previous_count(lake: LakeStorage, day: date) -> int | None:
    """Titles in the most recent earlier snapshot, or None if this is the first."""
    import datetime as dt

    for back in range(1, 8):
        earlier = day - dt.timedelta(days=back)
        key = f"{TITLE_PREFIX}/dt={earlier.isoformat()}/data.parquet"
        count = lake.count_rows(key)
        if count is not None:
            return count
    return None


# --------------------------------------------------------------------------------
# playback: the Playback Reporting plugin, one hour at a time
# --------------------------------------------------------------------------------
#
# Hourly, not daily, so a question asked Sunday evening about Sunday gets an answer
# rather than waiting for a job that runs after midnight. Full history is
# backfillable from 2025-02-14, the earliest row the plugin holds on this server.
#
# end_offset=1 makes the current hour a valid partition for the same reason camera's
# does: `jellyfin_playback_late_arrival_schedule` in definitions.py re-triggers the
# last few hours on every tick, because a session the plugin has not committed yet is
# invisible to a read that runs while it is still open -- a two-hour film started at
# minute five of its hour is still playing when that hour's own run executes. The
# re-read happens by re-materialising past hourly partitions, the same mechanism
# `camera_late_arrival_schedule` uses for days; there is no separate "read a wider
# window" path inside this asset, because a partition's own contract has to stay
# exactly its own hour for the reconciliation check below to mean anything.
PLAYBACK_START = "2025-02-14-00:00"
playback_partitions = HourlyPartitionsDefinition(start_date=PLAYBACK_START, end_offset=1)

PLAYBACK_PREFIX = layout.prefix("jellyfin_playback")

_PLAYBACK_SCHEMA = pa.schema(
    [
        # The partition's own hour, as a column: a reader who globs the prefix still
        # sees it, the same reason every other partition column here is stored rather
        # than left in the path alone.
        ("partition_hour", pa.timestamp("us", tz="UTC")),
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("started_at", pa.timestamp("us", tz="UTC")),
        ("user_id", pa.string()),
        ("user_name", pa.string()),
        ("item_id", pa.string()),
        ("item_type", pa.string()),
        ("item_name", pa.string()),
        # Parsed out of item_name for an Episode row -- see parse_series_name for why
        # this needs no join and no extra API call. Null for everything else.
        ("series_name", pa.string()),
        ("playback_method", pa.string()),
        ("client_name", pa.string()),
        ("device_name", pa.string()),
        ("play_duration_seconds", pa.int64()),
    ]
)

CHECK_PLAYBACK_RECONCILED = "playback_matches_the_plugins_own_count"


def _playback_key(row: dict):
    started_at = row.get("started_at")
    if started_at is None:
        return None
    # PlaybackActivity carries no id at all -- not even a rowid the API exposes -- so
    # this triple is the closest thing to a natural key: two genuinely different
    # sessions sharing a start timestamp, a user and an item does not happen.
    return (started_at, row.get("user_id"), row.get("item_id"))


def _playback_row_is_newer(candidate: dict, incumbent: dict) -> bool:
    new_at, old_at = candidate.get("_extracted_at"), incumbent.get("_extracted_at")
    if new_at is None:
        return False
    return old_at is None or new_at > old_at


def _merge_playback(
    existing: list[dict], incoming: list[dict], schema: pa.Schema
) -> tuple[list[dict], int]:
    """Union two reads of one hour, newest `_extracted_at` per (started_at, user, item).

    Same shape as `_merge` in `assets/camera.py`, generalised from a single `id`
    column to the composite key above because the plugin's table has no id to key on.
    A session's own row can legitimately change between two reads of the same hour:
    the plugin does not expose whether a session is still open, so an early read can
    see a small PlayDuration for a film that is still playing, and a later read the
    final one once it ends.
    """
    names = [f.name for f in schema]
    incoming_keys = {_playback_key(row) for row in incoming}
    merged: dict = {}

    for row in list(existing) + list(incoming):
        normalised = {name: row.get(name) for name in names}
        key = _playback_key(normalised)
        if key is None:
            continue
        incumbent = merged.get(key)
        if incumbent is None or _playback_row_is_newer(normalised, incumbent):
            merged[key] = normalised

    retained_only_in_lake = sum(1 for key in merged if key not in incoming_keys)
    return list(merged.values()), retained_only_in_lake


def _read_raw_rows(lake: LakeStorage, key: str, schema: pa.Schema) -> list[dict]:
    table = lake.read_table(key)
    return table.to_pylist() if table is not None else []


@asset(
    partitions_def=playback_partitions,
    group_name="jellyfin",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_PLAYBACK_RECONCILED,
            asset="jellyfin_playback_raw",
            description=(
                "Fails when the row count this run wrote for its own hour does not "
                "match what the plugin's own query just returned for that same hour. "
                "The two are computed from the same read, so a mismatch means the "
                "write or the merge dropped or duplicated something -- not that a "
                "session is still in progress, which is invisible to both sides of "
                "the comparison equally and only shows up once a later run re-reads "
                "the hour."
            ),
        ),
    ],
    description=(
        "One hour of Playback Reporting plugin rows: who watched what, on what "
        "device, for how long the player was open. Merged into the partition, never "
        "replaced -- see the module docstring on why a later read of the same hour "
        "can legitimately add rows a first read could not see."
    ),
)
def jellyfin_playback_raw(context: AssetExecutionContext, lake: LakeStorage):
    start, end = context.partition_time_window

    base_url = os.environ.get("JELLYFIN_URL", "")
    api_key = os.environ.get("JELLYFIN_API_KEY", "")
    if not base_url or not api_key:
        raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")

    offered = jellyfin_source.playback_window(start, end, base_url, api_key)
    source_count = len(offered)

    extracted_at = datetime.now(UTC)
    for row in offered:
        row["partition_hour"] = start
        row["dt"] = row["started_at"].date() if row["started_at"] else start.date()
        row["_extracted_at"] = extracted_at

    projected, _unmapped = project(offered, _PLAYBACK_SCHEMA)

    prefix = f"{PLAYBACK_PREFIX}/partition_hour={start:%Y-%m-%dT%H:00:00}"
    key = f"{prefix}/data.parquet"
    existing = _read_raw_rows(lake, key, _PLAYBACK_SCHEMA)
    merged, retained = _merge_playback(existing, projected, _PLAYBACK_SCHEMA)

    lake.delete_prefix(prefix)
    written = lake.write_table(key, pa.Table.from_pylist(merged, schema=_PLAYBACK_SCHEMA))

    context.log.info(
        "hour=%s..%s %d sessions offered by the plugin, %d in the partition "
        "(was %d, %d kept that the plugin no longer offers)",
        start, end, source_count, len(merged), len(existing), retained,
    )

    yield Output(
        {"sessions": len(merged)},
        metadata={
            "sessions": len(merged),
            "sessions_offered_by_plugin": source_count,
            "sessions_before_merge": len(existing),
            "sessions_only_in_lake": retained,
            "watch_seconds": sum(r.get("play_duration_seconds") or 0 for r in merged),
            "bytes_written": written,
            "extracted_at": MetadataValue.text(str(extracted_at)),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_PLAYBACK_RECONCILED,
        passed=len(merged) == source_count,
        severity=AssetCheckSeverity.ERROR,
        metadata={"warehouse_rows": len(merged), "plugin_count": source_count},
    )


# --------------------------------------------------------------------------------
# the server activity log
# --------------------------------------------------------------------------------

# Collection starts here rather than backfilling to the dawn of the log: nothing in
# the brief asked for the full [count withheld]-entry history, and a backfill of an API that
# offers only a floor (`minDate`) and no ceiling gets more expensive the further back
# it reaches -- see `activity_window` in parsing.jellyfin. Daily, and not end_offset=1:
# an activity day, like a log day, has to be over before it is complete.
ACTIVITY_START = "2026-09-14"
activity_partitions = DailyPartitionsDefinition(start_date=ACTIVITY_START)

ACTIVITY_PREFIX = layout.prefix("jellyfin_activity")

_ACTIVITY_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("_extracted_at", pa.timestamp("us", tz="UTC")),
        ("id", pa.int64()),
        ("date", pa.timestamp("us", tz="UTC")),
        ("type", pa.string()),
        ("name", pa.string()),
        ("overview", pa.string()),
        ("short_overview", pa.string()),
        ("user_id", pa.string()),
        ("item_id", pa.string()),
        ("severity", pa.string()),
        ("_unmapped", pa.string()),
    ]
)

CHECK_ACTIVITY_RECONCILED = "activity_row_count_matches_what_was_read"
CHECK_ACTIVITY_NO_UNMAPPED = "no_unmapped_fields"


@asset(
    partitions_def=activity_partitions,
    group_name="jellyfin",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_ACTIVITY_RECONCILED,
            asset="jellyfin_activity_raw",
            description=(
                "Fails when the row count written does not match the row count "
                "`activity_window` determined belongs to the day while paginating. "
                "The activity log API offers no single 'count for this day' endpoint "
                "-- only a floor (minDate) and a page -- so this cannot catch a "
                "pagination bug that skips a page; what it catches is the write or "
                "the schema projection silently dropping a row after it was counted."
            ),
        ),
        AssetCheckSpec(
            name=CHECK_ACTIVITY_NO_UNMAPPED,
            asset="jellyfin_activity_raw",
            description="Warns when the activity log sent a field the schema does not name.",
        ),
    ],
    description=(
        "One day of the server activity log: sign-ins, playback starts and stops, "
        "library and account changes. Replaced on every run -- the log is append-only "
        "at the source, so there is nothing to merge that a plain re-read would not "
        "already have."
    ),
)
def jellyfin_activity_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)
    base_url = os.environ.get("JELLYFIN_URL", "")
    api_key = os.environ.get("JELLYFIN_API_KEY", "")
    if not base_url or not api_key:
        raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")

    rows, unmapped = jellyfin_source.activity_window(day, base_url, api_key)
    collected_count = len(rows)
    extracted_at = datetime.now(UTC)
    for row in rows:
        row["_extracted_at"] = extracted_at

    projected, _more_unmapped = project(rows, _ACTIVITY_SCHEMA)

    prefix = f"{ACTIVITY_PREFIX}/dt={day.isoformat()}"
    lake.delete_prefix(prefix)
    written = lake.write_table(
        f"{prefix}/data.parquet", pa.Table.from_pylist(projected, schema=_ACTIVITY_SCHEMA)
    )

    context.log.info("dt=%s %d activity log entries", day, len(projected))
    if unmapped:
        context.log.warning(
            "dt=%s the activity log sent fields the schema does not name: %s",
            day, ", ".join(sorted(unmapped)),
        )

    yield Output(
        {"entries": len(projected)},
        metadata={
            "entries": len(projected),
            "bytes_written": written,
            "unmapped_fields": MetadataValue.text(", ".join(sorted(unmapped)) or "none"),
            "extracted_at": MetadataValue.text(str(extracted_at)),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_ACTIVITY_RECONCILED,
        passed=len(projected) == collected_count,
        severity=AssetCheckSeverity.ERROR,
        metadata={"written": len(projected), "collected": collected_count},
    )

    yield AssetCheckResult(
        check_name=CHECK_ACTIVITY_NO_UNMAPPED,
        passed=not unmapped,
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(unmapped)) or "none")},
    )


# --------------------------------------------------------------------------------
# the account list
# --------------------------------------------------------------------------------
#
# A daily snapshot, like camera_zones_raw: an account list has no history at the
# API, asking for it returns how accounts stand *now*, so a partition before
# collection began cannot be honestly filled and end_offset=1 makes today valid.
USERS_START = "2026-09-14"
users_partitions = DailyPartitionsDefinition(start_date=USERS_START, end_offset=1)

USERS_PREFIX = layout.prefix("jellyfin_users")

_USERS_SCHEMA = pa.schema(
    [
        ("dt", pa.date32()),
        ("id", pa.string()),
        ("name", pa.string()),
        ("is_administrator", pa.bool_()),
        ("is_disabled", pa.bool_()),
        ("last_login", pa.timestamp("us", tz="UTC")),
        ("last_activity", pa.timestamp("us", tz="UTC")),
        ("_unmapped", pa.string()),
    ]
)

CHECK_USERS_NO_UNMAPPED = "no_unmapped_fields"


@asset(
    partitions_def=users_partitions,
    group_name="jellyfin",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=15),
    check_specs=[
        AssetCheckSpec(
            name=CHECK_USERS_NO_UNMAPPED,
            asset="jellyfin_users_raw",
            description="Warns when `/Users` sent a field neither read nor declared not-stored.",
        ),
    ],
    description=(
        "Today's account list: name, admin flag, disabled flag, last seen. Nothing "
        "from configuration or password policy beyond those two flags."
    ),
)
def jellyfin_users_raw(context: AssetExecutionContext, lake: LakeStorage):
    day = date.fromisoformat(context.partition_key)
    base_url = os.environ.get("JELLYFIN_URL", "")
    api_key = os.environ.get("JELLYFIN_API_KEY", "")
    if not base_url or not api_key:
        raise ValueError("JELLYFIN_URL and JELLYFIN_API_KEY are required")

    raw_users = jellyfin_source.fetch_users(base_url, api_key)
    if not raw_users:
        raise ValueError(
            f"Jellyfin returned no users for {day}. An empty account list is an "
            "unreachable server, not a real state -- refusing to write it."
        )
    rows, unmapped = jellyfin_source.user_rows(raw_users, day)
    projected, _more_unmapped = project(rows, _USERS_SCHEMA)

    prefix = f"{USERS_PREFIX}/dt={day.isoformat()}"
    # Replaced, not merged: a snapshot describes the account list at a moment, and an
    # account removed from it should be absent rather than preserved.
    lake.delete_prefix(prefix)
    written = lake.write_table(
        f"{prefix}/data.parquet", pa.Table.from_pylist(projected, schema=_USERS_SCHEMA)
    )

    context.log.info("dt=%s %d accounts", day, len(projected))
    if unmapped:
        context.log.warning(
            "dt=%s /Users sent fields the schema does not name: %s",
            day, ", ".join(sorted(unmapped)),
        )

    yield Output(
        {"accounts": len(projected)},
        metadata={
            "accounts": len(projected),
            "administrators": sum(1 for r in rows if r.get("is_administrator")),
            "disabled": sum(1 for r in rows if r.get("is_disabled")),
            "bytes_written": written,
            "unmapped_fields": MetadataValue.text(", ".join(sorted(unmapped)) or "none"),
        },
    )

    yield AssetCheckResult(
        check_name=CHECK_USERS_NO_UNMAPPED,
        passed=not unmapped,
        severity=AssetCheckSeverity.WARN,
        metadata={"fields": MetadataValue.text(", ".join(sorted(unmapped)) or "none")},
    )
