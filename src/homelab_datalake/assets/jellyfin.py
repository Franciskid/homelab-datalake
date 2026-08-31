"""Jellyfin library assets.

A full snapshot per day, partitioned by date only -- one library, one server.

Two objects per partition rather than one: titles, and the title-to-genre pairs. A
title has many genres, so flattening them into the title row would mean either a
delimited string nobody can filter on or `genre_1, genre_2, genre_3`. The pair table is
the honest shape and becomes the bridge in the mart.
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

from homelab_datalake import layout
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
        "A daily snapshot of the movie library: metadata, codecs and ratings. No "
        "playback history, no file paths."
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
