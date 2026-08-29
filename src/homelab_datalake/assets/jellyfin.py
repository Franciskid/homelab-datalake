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

from homelab_datalake.parsing.jellyfin import snapshot
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

TITLE_PREFIX = "raw/jellyfin_titles"
GENRE_PREFIX = "raw/jellyfin_title_genres"

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
        ("studio", pa.string()),
        ("production_country", pa.string()),
        ("decade", pa.int32()),
    ]
)

_GENRE_SCHEMA = pa.schema(
    [("dt", pa.date32()), ("item_id", pa.string()), ("genre", pa.string())]
)

CHECK_NOT_SHRUNK = "library_has_not_shrunk_unexpectedly"
CHECK_TMDB_COVERAGE = "tmdb_id_coverage"


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

    title_prefix = f"{TITLE_PREFIX}/dt={day.isoformat()}"
    genre_prefix = f"{GENRE_PREFIX}/dt={day.isoformat()}"
    lake.delete_prefix(title_prefix)
    lake.delete_prefix(genre_prefix)

    written = lake.write_table(
        f"{title_prefix}/data.parquet", pa.Table.from_pylist(result.titles, schema=_TITLE_SCHEMA)
    )
    written += lake.write_table(
        f"{genre_prefix}/data.parquet", pa.Table.from_pylist(result.genres, schema=_GENRE_SCHEMA)
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
            "tmdb_coverage_pct": MetadataValue.float(round(100 * coverage, 2)),
            "pages_fetched": result.pages,
            "bytes_written": written,
            "destination": MetadataValue.path(f"{lake.describe()}/{title_prefix}/data.parquet"),
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
