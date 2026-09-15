"""A synthetic warehouse shaped like the real marts.

The tests need a database with the columns the semantic layer names, and they must
not need the homelab to get one. This builds a small warehouse with the same column
names and types as the dbt marts, so a dimension or measure that references a column
that does not exist fails here rather than in production.

It is a stand-in for the *shape*, not for the data: the row values are arithmetic on
a counter, chosen so every branch (nulls, failures, weekends, unscored events) is
represented rather than to look plausible.
"""

import duckdb
import pytest

_LLM = """
CREATE OR REPLACE TABLE marts.fct_llm_call AS
SELECT
    'c' || i AS call_key,
    (DATE '2026-08-01' + (i % 20)) AS partition_date,
    (TIMESTAMPTZ '2026-08-01 09:00:00Z' + INTERVAL (i) HOUR) AS started_at,
    date_trunc('hour', TIMESTAMPTZ '2026-08-01 09:00:00Z' + INTERVAL (i) HOUR) AS started_hour,
    CASE WHEN i % 3 = 0 THEN 'claude-opus-4'
         WHEN i % 3 = 1 THEN 'gpt-4o' ELSE 'llama3' END AS model,
    CASE WHEN i % 3 = 2 THEN 'local' ELSE 'frontier' END AS model_group,
    CASE WHEN i % 3 = 0 THEN 'anthropic'
         WHEN i % 3 = 1 THEN 'openai' ELSE 'ollama' END AS provider,
    CASE WHEN i % 4 = 0 THEN 'agent' ELSE 'chat' END AS workload,
    'completion' AS call_type,
    CAST(NULL AS VARCHAR) AS mcp_tool,
    CASE WHEN i % 17 = 0 THEN 'failure' ELSE 'success' END AS status,
    (i % 3 = 2) AS is_self_hosted,
    (i % 5 = 0) AS was_cache_hit,
    (i % 7) * 0.01 AS spend_usd,
    100 + i AS total_tokens, 60 + i AS prompt_tokens, 40 AS completion_tokens,
    ((i % 7) * 0.01) / (100 + i) AS usd_per_token,
    500 + (i % 50) * 20 AS duration_ms, 120 + (i % 30) AS time_to_first_token_ms,
    'team-' || (i % 2) AS team_id,
    'eu' || (i % 11) AS end_user_hash,
    'kh' || (i % 4) AS api_key_hash,
    'ch' || (i % 13) AS client_hash,
    CASE WHEN i % 8 = 0 THEN 'unknown' ELSE 'resolved' END AS client_trust,
    CASE WHEN i % 8 <> 0 THEN 'FR' END AS geo_country_code,
    CASE WHEN i % 8 <> 0 THEN 'Example AS' END AS geo_as_org,
    's' || (i % 9) AS session_id
FROM (SELECT i::INTEGER AS i FROM range(0, 200) t(i));
"""

_HTTP = """
CREATE OR REPLACE TABLE marts.fct_http_request AS
SELECT
    'r' || i AS request_key,
    (DATE '2026-08-01' + (i % 20)) AS partition_date,
    'primary' AS npm_instance,
    (TIMESTAMPTZ '2026-08-01 00:00:00Z' + INTERVAL (i) MINUTE) AS requested_at,
    date_trunc('hour', TIMESTAMPTZ '2026-08-01 00:00:00Z' + INTERVAL (i) MINUTE) AS requested_hour,
    CASE WHEN i % 4 = 0 THEN 'tv.example.org' ELSE 'data.example.org' END AS request_host,
    '/api/v1/item/' || i AS path,
    '/api' AS path_group, 'GET' AS method, 'https' AS scheme,
    CASE WHEN i % 9 = 0 THEN 'tab=' || i ELSE NULL END AS query_string,
    CASE WHEN i % 23 = 0 THEN 500 ELSE 200 END AS status,
    CASE WHEN i % 23 = 0 THEN 'server_error' ELSE 'success' END AS status_class,
    CASE WHEN i % 23 = 0 THEN 502 ELSE 200 END AS upstream_status,
    '10.0.0.' || (i % 5) || ':8080' AS upstream_target,
    CASE WHEN i % 6 = 0 THEN 'bot' ELSE 'browser' END AS client_kind,
    CASE WHEN i % 5 = 0 THEN 'cdn_edge' ELSE 'resolved' END AS client_trust,
    4 AS client_ip_version, false AS is_internal, (i % 6 = 0) AS is_automated,
    'Mozilla/5.0 (test ' || (i % 3) || ')' AS user_agent,
    -- RFC 5737 documentation range -- see tests/test_no_real_addresses_in_fixtures.py.
    '203.0.113.' || (i % 40) AS client_address,
    'h' || (i % 40) AS client_hash, 'n' || (i % 12) AS client_network,
    'FR' AS geo_country_code, 'France' AS geo_country_name, 'Paris' AS geo_city,
    64500 + (i % 3) AS geo_asn,
    'Example AS' AS geo_as_org, 1000 + i AS bytes_sent,
    CASE WHEN i % 30 = 0 THEN 'legacy' ELSE 'json' END AS log_format
FROM (SELECT i::INTEGER AS i FROM range(0, 300) t(i));
"""

# The camera fixture carries all 78 columns of the real mart, and it is the one that
# has to: the whole reason grain='event' exists is that a listing of openings needs
# happened_at, state_changed and the counts, and a fixture missing any of them would
# let a test pass over a semantic layer that cannot answer the question.
_CAMERA = """
CREATE OR REPLACE TABLE marts.fct_camera_event AS
WITH base AS (
    SELECT
        i,
        (DATE '2026-08-01' + (i % 20)) AS day,
        (TIMESTAMPTZ '2026-08-01 05:30:00Z' + INTERVAL (i * 37) MINUTE) AS ts,
        (i % 4 = 0) AS corrected
    FROM (SELECT i::INTEGER AS i FROM range(0, 250) t(i))
)
SELECT
    'e' || i AS event_id,
    day AS partition_date,
    'cam-' || (i % 2) AS camera_id, 'zone-' || (i % 3) AS zone_id,
    CASE WHEN i % 3 = 0 THEN 'driveway'
         WHEN i % 3 = 1 THEN 'garden' ELSE 'door' END AS zone_name,
    'rule-' || (i % 3) AS rule_id,
    CASE WHEN i % 3 = 0 THEN 'state' ELSE 'presence' END AS rule_type,
    CASE WHEN i % 10 = 0 THEN 'candidate'
         WHEN i % 3 = 0 THEN 'confirmed' ELSE 'dismissed' END AS lifecycle,
    (i % 3 = 0 AND i % 10 <> 0) AS is_confirmed,
    (i % 10 = 0) AS is_awaiting_analysis,
    (i % 3 <> 0 AND i % 10 <> 0) AS is_dismissed,
    ts AS started_at,
    CASE WHEN i % 5 <> 0 THEN ts - INTERVAL 3 SECOND END AS occurred_at,
    coalesce(CASE WHEN i % 5 <> 0 THEN ts - INTERVAL 3 SECOND END, ts) AS happened_at,
    ts + INTERVAL (10 + (i % 40)) SECOND AS completed_at,
    CASE WHEN i % 3 = 0 THEN ts END AS state_observed_at,
    CASE WHEN i % 3 = 0 THEN 'vision' END AS state_source,
    10 + (i % 40) AS duration_seconds,
    date_trunc('hour', ts) AS started_hour,
    (i % 24) AS started_hour_of_day,
    'Monday' AS started_weekday, (i % 7 > 4) AS is_weekend,
    CAST(NULL AS VARCHAR) AS direction,
    CASE WHEN i % 3 = 0 THEN (CASE WHEN i % 6 = 0 THEN 'open' ELSE 'closed' END) END AS zone_state,
    CASE WHEN i % 3 = 0 THEN (CASE WHEN i % 6 = 0 THEN 'closed' ELSE 'open' END) END
        AS previous_zone_state,
    (i % 3 = 0 AND i % 9 <> 0) AS state_changed,
    (i % 5) AS count_person, (i % 3) AS count_vehicle, (i % 7 = 0)::INTEGER AS count_animal,
    0 AS count_other, (i % 5) + (i % 3) + (i % 7 = 0)::INTEGER AS count_total,
    CASE WHEN i % 10 <> 0 THEN (i % 100) / 100.0 END AS confidence,
    (i % 11 = 0) AS is_unusual, (i % 4 = 0) AS should_notify,
    CASE WHEN i % 3 <> 0 THEN 'semantic_no_event' END AS dismissal_reason,
    CASE WHEN i % 31 = 0 THEN 'e' || (i - 1) END AS duplicate_of,
    (i % 31 = 0) AS is_duplicate,
    'Something moved near the ' || (i % 3) AS summary,
    corrected AS was_corrected,
    CASE WHEN corrected THEN ts + INTERVAL 2 HOUR END AS corrected_at,
    CASE WHEN corrected THEN 'Alex Doe' END AS corrected_by_name,
    CASE WHEN corrected THEN 'ph' || (i % 2) END AS corrected_by_hash,
    CASE WHEN corrected THEN 'candidate' END AS machine_lifecycle,
    CAST(NULL AS VARCHAR) AS machine_direction,
    CASE WHEN corrected THEN 'open' END AS machine_zone_state,
    CASE WHEN corrected THEN 'closed' END AS machine_previous_zone_state,
    CASE WHEN corrected THEN ts END AS machine_state_observed_at,
    CAST(NULL AS VARCHAR) AS machine_dismissal_reason,
    CAST(NULL AS VARCHAR) AS machine_duplicate_of,
    CASE WHEN corrected THEN 'The model was unsure' END AS machine_summary,
    CASE WHEN corrected THEN 0.4 END AS machine_confidence,
    CASE WHEN corrected THEN false END AS machine_should_notify,
    CASE WHEN corrected THEN 1 END AS machine_count_person,
    CASE WHEN corrected THEN 0 END AS machine_count_vehicle,
    CASE WHEN corrected THEN 0 END AS machine_count_animal,
    CASE WHEN corrected THEN 0 END AS machine_count_other,
    (corrected AND i % 8 = 0) AS lifecycle_was_corrected,
    (corrected AND i % 8 <> 0) AS zone_state_was_corrected,
    CASE WHEN i % 2 = 0 THEN 'clip-' || i END AS clip_id,
    CASE WHEN i % 12 = 0 THEN 'clip-t-' || i END AS transition_clip_id,
    CASE WHEN i % 2 = 0 THEN 'motion' END AS clip_kind,
    CASE WHEN i % 2 = 0 THEN 'Clip ' || i END AS clip_title,
    CASE WHEN i % 2 = 0 THEN 20 + (i % 30) END AS clip_duration_seconds,
    CASE WHEN i % 2 = 0 THEN 1000000 + i * 913 END AS clip_size_bytes,
    CASE WHEN i % 2 = 0 THEN 'high' END AS clip_storage_quality,
    CASE WHEN i % 2 = 0 THEN (i % 20 = 0) END AS clip_is_archived,
    CASE WHEN i % 2 = 0 THEN false END AS clip_is_deleted,
    CASE WHEN i % 2 = 0 THEN 'local' END AS clip_ai_source,
    CASE WHEN i % 2 = 0 THEN 'object' END AS clip_ai_type,
    CASE WHEN i % 2 = 0 THEN (i % 90) / 100.0 END AS clip_local_confidence,
    CASE WHEN i % 2 = 0 THEN (i % 3) ELSE 0 END AS clip_annotation_count,
    (i % 2 = 0) AS has_clip,
    CAST(NULL AS VARCHAR) AS backfill_json,
    CAST(NULL AS VARCHAR) AS repair_json,
    CAST(NULL AS VARCHAR) AS restoration_json,
    CAST(NULL AS VARCHAR) AS timing_correction_json,
    CAST(NULL AS VARCHAR) AS unmapped_json,
    ts + INTERVAL 1 DAY AS extracted_at
FROM base;
"""

# Three snapshot dates on purpose: the rule that a range selects one snapshot rather
# than summing across them is only testable when there is more than one.
_LIBRARY = """
CREATE OR REPLACE TABLE marts.fct_library_title AS
SELECT
    'k' || i || '-' || d AS title_key, 'item' || i AS item_id,
    (DATE '2026-08-25' + d) AS snapshot_date,
    'Film ' || i AS title, 'Original ' || i AS original_title,
    1990 + (i % 30) AS production_year,
    ((1990 + (i % 30)) / 10) * 10 AS decade,
    (DATE '1990-01-01' + (i % 30) * 365) AS premiere_date,
    (TIMESTAMPTZ '2026-01-01 00:00:00Z' + INTERVAL (i) DAY) AS added_at,
    CASE WHEN i % 4 = 0 THEN '4K'
         WHEN i % 4 = 1 THEN '1080p' ELSE '720p' END AS resolution_class,
    'h264' AS video_codec, 'aac' AS audio_codec, 'mkv' AS container,
    CASE WHEN i % 4 = 0 THEN 2160 WHEN i % 4 = 1 THEN 1080 ELSE 720 END AS video_height,
    CASE WHEN i % 4 = 0 THEN 3840 WHEN i % 4 = 1 THEN 1920 ELSE 1280 END AS video_width,
    CASE WHEN i % 3 = 0 THEN 6 ELSE 2 END AS audio_channels,
    'PG' AS official_rating, 'Studio ' || (i % 5) AS studio,
    -- From Jellyfin, so populated whether or not TMDB enrichment is configured.
    CASE WHEN i % 40 <> 0 THEN 'US' END AS production_country,
    CASE WHEN i % 50 = 0 THEN 1700 ELSE 90 + (i % 60) END AS runtime_minutes,
    (i % 50 <> 0) AS is_runtime_plausible, (i % 3 = 0) AS is_long_feature,
    (i % 2 = 0) AS is_high_bit_depth, (i % 20 <> 0) AS is_enrichable,
    CASE WHEN i % 6 <> 0 THEN 5.0 + (i % 50) / 10.0 END AS community_rating,
    CASE WHEN i % 13 = 0 THEN 70.0 + (i % 25) END AS critic_rating,
    CASE WHEN i % 20 <> 0 THEN 5000 + i END AS tmdb_id,
    CASE WHEN i % 20 <> 0 THEN 'tt' || (1000000 + i) END AS imdb_id,
    -- TMDB enrichment: only the figures Jellyfin has no field for. Null for some
    -- rows on purpose -- unenriched titles are a supported state, not a broken one,
    -- and every measure has to survive them.
    CASE WHEN i % 7 <> 0 THEN 'en' END AS original_language,
    CASE WHEN i % 9 = 0 THEN 20000000 END AS budget_usd,
    CASE WHEN i % 9 = 0 THEN 55000000 END AS revenue_usd,
    CASE WHEN i % 9 = 0 THEN 2.75 END AS revenue_to_budget,
    CASE WHEN i % 7 <> 0 THEN (i % 90) / 3.0 END AS tmdb_popularity,
    CASE WHEN i % 7 <> 0 THEN 100 + i END AS tmdb_vote_count,
    (i % 7 <> 0) AS is_enriched
FROM (SELECT i::INTEGER AS i FROM range(0, 120) t(i)),
     (SELECT d::INTEGER AS d FROM range(0, 3) s(d));
"""

# One session in five is an episode, which never resolves against the movie-only
# library or carries a genre -- the same split the real server shows. A third of
# sessions belong to an account id the fixture never names, the same shape measured
# against the real server: deleted or system accounts outnumber live ones in the raw
# history, so a test that only ever sees a resolved person would not be exercising
# the common case.
_JELLYFIN_PLAYBACK = """
CREATE OR REPLACE TABLE marts.fct_jellyfin_playback AS
WITH base AS (
    SELECT
        i,
        (TIMESTAMPTZ '2026-08-01 05:00:00Z' + INTERVAL (i * 53) MINUTE) AS ts,
        (i % 5 = 0) AS is_ep
    FROM (SELECT i::INTEGER AS i FROM range(0, 180) t(i))
)
SELECT
    'pk' || i AS playback_key,
    CAST(ts AS DATE) AS partition_date,
    date_trunc('hour', ts) AS partition_hour,
    ts + INTERVAL 5 MINUTE AS extracted_at,
    ts AS started_at,
    date_trunc('hour', ts) AS started_hour,
    (i % 24) AS started_hour_of_day,
    ['Monday', 'Tuesday', 'Wednesday'][(i % 3) + 1] AS started_weekday,
    (i % 7 > 4) AS is_weekend,
    CASE WHEN i % 3 <> 0 THEN 'user-' || (i % 6) END AS user_id,
    CASE WHEN i % 3 <> 0 THEN 'person' || (i % 6) END AS user_name,
    'item-' || (i % 40) AS item_id,
    CASE WHEN is_ep THEN 'Episode'
         WHEN i % 11 = 0 THEN 'Video'
         WHEN i % 13 = 0 THEN 'TvChannel'
         ELSE 'Movie' END AS item_type,
    CASE WHEN is_ep THEN 'Show ' || (i % 4) || ' - s01e0' || ((i % 9) + 1) || ' - Episode ' || i
         ELSE 'Film ' || (i % 40) END AS title,
    -- Two-thirds of episodes resolve by id against the library catalogue, a third
    -- fall back to the name parse -- both paths need a row in the fixture. The real
    -- server's own split, measured 2026-09-15, was [rate withheld] resolved to [rate withheld] fallback.
    CASE WHEN is_ep AND i % 3 <> 0 THEN 'series-' || (i % 4) END AS series_id,
    CASE WHEN is_ep THEN 'Show ' || (i % 4) END AS series_name,
    (is_ep AND i % 3 <> 0) AS series_resolved_by_id,
    CASE WHEN is_ep AND i % 3 <> 0 THEN 1 END AS season_number,
    CASE WHEN is_ep AND i % 3 <> 0 THEN (i % 9) + 1 END AS episode_number,
    is_ep AS is_episode,
    CASE WHEN NOT is_ep AND i % 6 <> 0
         THEN ['Action', 'Drama', 'Comedy'][(i % 3) + 1] END AS genre,
    CASE WHEN i % 4 = 0 THEN 'DirectPlay' ELSE 'Transcode (v:hevc a:aac)' END AS playback_method,
    ['Jellyfin Web', 'Android TV', 'Swiftfin'][(i % 3) + 1] AS client_name,
    ['Chrome', 'Safari', 'Shield'][(i % 3) + 1] AS device_name,
    30 + (i % 7000) AS play_duration_seconds,
    (NOT is_ep AND i % 11 <> 0) AS is_in_library,
    CASE WHEN NOT is_ep AND i % 11 <> 0 THEN 5.0 + (i % 40) / 10.0 END AS library_community_rating
FROM base;
"""

# Most entries name no account at all -- a library scan concerns nobody -- and among
# the ones that do, most name an id the account snapshot no longer holds. Both
# measured against the real server, not guessed.
_JELLYFIN_ACTIVITY = """
CREATE OR REPLACE TABLE marts.fct_jellyfin_activity AS
WITH base AS (
    SELECT i, (TIMESTAMPTZ '2026-08-01 00:00:00Z' + INTERVAL (i * 17) MINUTE) AS ts
    FROM (SELECT i::INTEGER AS i FROM range(0, 220) t(i))
)
SELECT
    i AS activity_id,
    CAST(ts AS DATE) AS partition_date,
    ts + INTERVAL 1 MINUTE AS extracted_at,
    ts AS happened_at,
    date_trunc('hour', ts) AS happened_hour,
    (i % 24) AS happened_hour_of_day,
    ['Monday', 'Tuesday', 'Wednesday'][(i % 3) + 1] AS happened_weekday,
    (i % 7 > 4) AS is_weekend,
    ['VideoPlayback', 'SessionStarted', 'AuthenticationSucceeded', 'LibraryScan'][(i % 4) + 1]
        AS activity_type,
    'entry ' || i AS name,
    CASE WHEN i % 19 = 0 THEN 'A longer note.' END AS overview,
    CASE WHEN i % 5 = 0 THEN 'Adresse IP : 10.0.0.' || (i % 20) END AS short_overview,
    CASE WHEN i % 3 = 0 THEN 'user-' || (i % 6)
         WHEN i % 3 = 1 THEN 'ghost-' || i END AS user_id,
    CASE WHEN i % 3 = 0 THEN 'person' || (i % 6) END AS user_name,
    CASE WHEN i % 4 = 0 THEN 'item-' || (i % 40) END AS item_id,
    CASE WHEN i % 29 = 0 THEN 'Error'
         WHEN i % 11 = 0 THEN 'Warning' ELSE 'Information' END AS severity,
    (i % 29 = 0) AS is_error,
    (i % 11 = 0 AND i % 29 <> 0) AS is_warning,
    CAST(NULL AS VARCHAR) AS unmapped_json
FROM base;
"""


def build_marts(path: str) -> None:
    con = duckdb.connect(path)
    con.execute("SET TimeZone='UTC';")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    for statement in (_LLM, _HTTP, _CAMERA, _LIBRARY, _JELLYFIN_PLAYBACK, _JELLYFIN_ACTIVITY):
        con.execute(statement)
    con.close()


@pytest.fixture(scope="session")
def marts_db(tmp_path_factory) -> str:
    # Not "marts.duckdb": DuckDB names the catalog after the file, and a catalog
    # called `marts` is ambiguous with the schema of the same name. The real serving
    # file avoids this by being called serving.duckdb.
    path = str(tmp_path_factory.mktemp("warehouse") / "serving.duckdb")
    build_marts(path)
    return path


@pytest.fixture
def con(marts_db):
    connection = duckdb.connect(marts_db, read_only=True)
    connection.execute("SET TimeZone='UTC';")
    yield connection
    connection.close()
