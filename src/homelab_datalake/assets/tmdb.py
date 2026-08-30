"""TMDB enrichment for the library: the few figures Jellyfin has no field for.

Deliberately small. Jellyfin queries TMDB itself during a scan, so the rating, the
original title, the production country and the release date are already in the daily
snapshot -- only budget, revenue, popularity, vote count and original language are
not. Fetching the rest again would put an API key between the pipeline and data it
already had.

Unpartitioned, and that is the honest shape. Every other asset here records an
observation of a day; this one is a *lookup cache* of reference data that changes on
TMDB's schedule, not on ours. Writing a daily partition of it would record [count withheld]
identical rows every night and claim they were a day's worth of news.

So it is cumulative: read what has already been enriched, ask TMDB only about ids that
are new or stale, and write the whole table back. Re-running is therefore cheap rather
than free, which is the right trade for something on the other end of a rate limit.

Without `TMDB_API_KEY` the asset still runs and writes an empty table with the right
columns, so the dbt models downstream compile and the library marts simply carry null
enrichment. Same contract as geo.
"""

from datetime import UTC, datetime

import duckdb
import pyarrow as pa
from dagster import AssetExecutionContext, MetadataValue, Output, RetryPolicy, asset

from homelab_datalake import layout
from homelab_datalake.assets.jellyfin import TITLE_PREFIX, jellyfin_library_raw
from homelab_datalake.parsing.tmdb import client_from_env
from homelab_datalake.resources import LakeStorage

TMDB_PREFIX = layout.prefix("tmdb_titles")

# A run asks TMDB about at most this many titles. The first run covers the library in
# a couple of minutes; after that it is a handful of new films a night. The cap exists
# so a mistake -- a cleared cache, a mangled id column -- costs one bounded run rather
# than an afternoon of requests.
MAX_LOOKUPS_PER_RUN = 1_500

# Popularity and vote counts drift; budget and language do not. A month is chosen for
# the drifting half, and nothing here is time-critical enough to justify less.
REFRESH_AFTER_DAYS = 30

# Five columns, not thirteen. The rest were cut because Jellyfin already carries
# them -- see the note at the top of parsing/tmdb.py.
_SCHEMA = pa.schema(
    [
        ("tmdb_id", pa.int64()),
        ("_enriched_at", pa.timestamp("us", tz="UTC")),
        ("original_language", pa.string()),
        ("budget_usd", pa.int64()),
        ("revenue_usd", pa.int64()),
        ("popularity", pa.float64()),
        ("vote_count", pa.int64()),
    ]
)


def _wanted_ids(lake: LakeStorage) -> list[int]:
    """Every TMDB id the current library snapshot holds.

    Read straight from the Parquet rather than through the warehouse, so this asset
    depends on the library landing and not on the warehouse being rebuilt first.
    """
    pattern = lake.duckdb_scan_target(TITLE_PREFIX).replace("'", "''")
    con = duckdb.connect()
    try:
        lake.configure_duckdb(con)
        rows = con.execute(
            # Only the newest snapshot: an id that has left the library does not need
            # refreshing, and enriching it again would be work for a row nothing joins.
            f"""
            WITH titles AS (
                SELECT * FROM read_parquet('{pattern}', union_by_name=true)
            )
            SELECT DISTINCT try_cast(tmdb_id AS BIGINT) AS id
            FROM titles
            WHERE dt = (SELECT max(dt) FROM titles)
              AND tmdb_id IS NOT NULL
            """  # noqa: S608 - the pattern is configuration, escaped above
        ).fetchall()
    except duckdb.IOException:
        return []
    finally:
        con.close()
    return sorted({r[0] for r in rows if r[0] is not None})


def _existing(lake: LakeStorage) -> dict[int, dict]:
    pattern = lake.duckdb_scan_target(TMDB_PREFIX).replace("'", "''")
    con = duckdb.connect()
    try:
        lake.configure_duckdb(con)
        result = con.execute(
            f"SELECT * FROM read_parquet('{pattern}', union_by_name=true)"  # noqa: S608
        )
        columns = [d[0] for d in result.description]
        return {
            row[columns.index("tmdb_id")]: dict(zip(columns, row, strict=True))
            for row in result.fetchall()
        }
    except duckdb.IOException:
        return {}  # nothing enriched yet
    finally:
        con.close()


def _needs_refresh(row: dict, now: datetime) -> bool:
    enriched = row.get("_enriched_at")
    if enriched is None:
        return True
    return (now - enriched).days >= REFRESH_AFTER_DAYS


@asset(
    deps=[jellyfin_library_raw],
    group_name="jellyfin",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    description=(
        "The four figures Jellyfin has no field for -- budget, revenue, popularity and "
        "vote count -- plus original language, by key lookup on the id Jellyfin already "
        "stored. Everything else people reach for TMDB to get is already in the library "
        "snapshot. Optional: with no API key the table is empty and these columns are "
        "null, while the rest of the library data is unaffected."
    ),
)
def tmdb_titles_raw(context: AssetExecutionContext, lake: LakeStorage) -> Output[dict]:
    now = datetime.now(UTC)
    client = client_from_env()
    known = _existing(lake)

    if client is None:
        # An empty table with the right columns, not a missing one: dbt reads column
        # names at compile time, so a missing table fails the build with a binder error
        # that says nothing about the real cause.
        lake.delete_prefix(TMDB_PREFIX)
        written = lake.write_table(
            f"{TMDB_PREFIX}/data.parquet", pa.Table.from_pylist([], schema=_SCHEMA)
        )
        context.log.info(
            "TMDB_API_KEY is not set -- wrote an empty enrichment table. Only budget, "
            "revenue, popularity, vote count and original language are affected; "
            "ratings, titles, countries and dates come from Jellyfin and are unchanged."
        )
        return Output(
            {"enriched": 0, "configured": False},
            metadata={"configured": False, "rows": 0, "bytes_written": written},
        )

    wanted = _wanted_ids(lake)
    stale = [i for i in wanted if i not in known or _needs_refresh(known[i], now)]
    to_fetch = stale[:MAX_LOOKUPS_PER_RUN]

    context.log.info(
        "%d titles carry a TMDB id, %d already enriched, %d stale or new, fetching %d",
        len(wanted), len(known), len(stale), len(to_fetch),
    )

    fetched = 0
    missing = 0
    for tmdb_id in to_fetch:
        facts = client.movie(tmdb_id)
        if facts is None:
            # TMDB does not have it. Recorded as a row with nulls rather than skipped,
            # so the next run does not ask again every night for an id that will never
            # resolve.
            known[tmdb_id] = {"tmdb_id": tmdb_id, "_enriched_at": now,
                              **{f.name: None for f in _SCHEMA
                                 if f.name not in ("tmdb_id", "_enriched_at")}}
            missing += 1
            continue
        known[tmdb_id] = {
            "tmdb_id": facts.tmdb_id,
            "_enriched_at": now,
            "original_language": facts.original_language,
            "budget_usd": facts.budget_usd,
            "revenue_usd": facts.revenue_usd,
            "popularity": facts.popularity,
            "vote_count": facts.vote_count,
        }
        fetched += 1

    names = [f.name for f in _SCHEMA]
    rows = [{name: row.get(name) for name in names} for row in known.values()]
    lake.delete_prefix(TMDB_PREFIX)
    written = lake.write_table(
        f"{TMDB_PREFIX}/data.parquet", pa.Table.from_pylist(rows, schema=_SCHEMA)
    )

    with_revenue = sum(1 for r in rows if r.get("revenue_usd"))
    return Output(
        {"enriched": fetched, "configured": True},
        metadata={
            "configured": True,
            "rows": len(rows),
            "fetched_this_run": fetched,
            "not_found_on_tmdb": missing,
            "still_outstanding": max(0, len(stale) - len(to_fetch)),
            "titles_with_revenue": with_revenue,
            "bytes_written": written,
            "source": MetadataValue.text("https://api.themoviedb.org/3/movie/{id}"),
        },
    )
