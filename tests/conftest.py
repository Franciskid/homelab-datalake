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
    'fraserv' AS npm_instance,
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
    CASE WHEN i % 7 = 0 THEN NULL ELSE i % 3 = 0 END AS is_internal_service,
    CASE WHEN i % 7 = 0 THEN 'unresolved' WHEN i % 3 = 0 THEN 'internal_service' ELSE 'unmanaged_network' END AS traffic_origin,
    CASE WHEN i % 3 = 0 THEN 'primary' END AS origin_edge,
    CASE WHEN i % 3 = 0 AND i % 7 <> 0 THEN 'monitoring' END AS origin_service,
    CASE WHEN i % 7 = 0 THEN 'unknown' WHEN i % 3 = 0 THEN 'inferred' ELSE 'network_only' END AS origin_confidence,
    'Synthetic classification evidence' AS origin_evidence,
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
    -- server's own split, measured 2026-09-15, was 92.5% resolved to 7.5% fallback.
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

# One search in six is the plugin's own AI popup (kind='ai'); the rest are Jellyfin's
# own endpoints. One in nine carries no user_id at all -- an API-key search -- and the
# semantic branches (enabled/ran/skipped/mode/duration/hits/matches) are all null
# together whenever semantic_ran is false, the same shape the real endpoint sends.
_JELLYFIN_SEARCHES = """
CREATE OR REPLACE TABLE marts.fct_jellyfin_search AS
WITH base AS (
    SELECT
        i,
        (TIMESTAMPTZ '2026-09-19 05:00:00Z' + INTERVAL (i * 41) MINUTE) AS ts,
        (i % 6 = 0) AS is_ai,
        (i % 13 <> 0) AS semantic_configured,
        (i % 13 <> 0 AND i % 4 <> 0) AS semantic_did_run
    FROM (SELECT i::INTEGER AS i FROM range(0, 200) t(i))
)
SELECT
    i AS search_key,
    CAST(ts AS DATE) AS partition_date,
    date_trunc('hour', ts) AS partition_hour,
    ts + INTERVAL 2 SECOND AS extracted_at,
    ts AS searched_at,
    date_trunc('hour', ts) AS searched_hour,
    (i % 24) AS searched_hour_of_day,
    ['Monday', 'Tuesday', 'Wednesday'][(i % 3) + 1] AS searched_weekday,
    (i % 7 > 4) AS is_weekend,
    CASE WHEN is_ai THEN 'ai' ELSE 'search' END AS kind,
    is_ai AS is_ai_search,
    CASE WHEN is_ai THEN '/AiSearch/Ask'
         ELSE ['/Items', '/Search/Hints', '/Persons'][(i % 3) + 1] END AS endpoint,
    'GET' AS method,
    CASE WHEN i % 29 = 0 THEN 500 ELSE 200 END AS status_code,
    20 + (i % 400) AS duration_ms,
    CASE WHEN i % 17 <> 0 THEN 'term ' || (i % 50) END AS search_term,
    CASE WHEN i % 9 <> 0 THEN 'user-' || (i % 6) END AS user_id,
    CASE WHEN i % 9 <> 0 THEN 'person' || (i % 6) END AS user_name,
    (i % 9 = 0) AS is_api_key,
    ['Jellyfin Web', 'Android TV', 'Swiftfin'][(i % 3) + 1] AS client,
    '12.1.0' AS client_version,
    ['Chrome', 'Shield', 'iPhone'][(i % 3) + 1] AS device,
    'device-' || (i % 10) AS device_id,
    CASE WHEN i % 5 <> 0 THEN '192.0.2.' || (i % 200) END AS remote_address,
    CASE WHEN i % 5 <> 0 THEN 'ch' || (i % 40) END AS client_hash,
    CASE WHEN i % 5 <> 0 THEN '192.0.2.0/24' END AS client_network,
    CASE WHEN i % 5 <> 0 THEN 'resolved' ELSE 'unknown' END AS client_trust,
    'Mozilla/5.0 (Test) Jellyfin/1.0' AS user_agent,
    CASE WHEN i % 3 = 0 THEN '{"searchTerm": "x"}' END AS parameters_json,
    CASE WHEN i % 11 = 0 THEN 'folder-' || (i % 4) END AS parent_id,
    CASE WHEN i % 11 = 0 THEN 'Library ' || (i % 4) END AS parent_name,
    (i % 4) AS result_count,
    (i % 4) AS total_record_count,
    (i % 4) AS results_logged,
    (i % 4 = 0) AS had_no_results,
    semantic_configured AS semantic_enabled,
    semantic_did_run AS semantic_ran,
    CASE WHEN semantic_configured AND NOT semantic_did_run THEN 'provider_unreachable' END
        AS semantic_skipped_reason,
    CASE WHEN semantic_did_run THEN 'remote' END AS semantic_mode,
    CASE WHEN semantic_did_run THEN 40 + (i % 200) END AS semantic_duration_ms,
    CASE WHEN semantic_did_run THEN 20 + (i % 60) END AS semantic_raw_hits,
    CASE WHEN semantic_did_run THEN (i % 4) END AS semantic_matches,
    CASE WHEN i % 47 = 0 THEN 'timed out' END AS semantic_error,
    CASE WHEN is_ai THEN ['normal', 'surprise'][(i % 2) + 1] END AS mode,
    CASE WHEN is_ai THEN 'Here are a few picks that match what you described.' END AS answer,
    CAST(NULL AS VARCHAR) AS unmapped_json
FROM base;
"""

# One row per result, zero to three per search -- a search that matched nothing has
# no result rows at all, the same as a real search whose result_count is zero. Only
# the top-ranked result of a search ever carries a semantic score, the same shape the
# real endpoint sends: semantic re-ranks the field, it does not score every row.
_JELLYFIN_SEARCH_RESULTS = """
CREATE OR REPLACE TABLE marts.fct_jellyfin_search_result AS
WITH base AS (
    SELECT
        i,
        (TIMESTAMPTZ '2026-09-19 05:00:00Z' + INTERVAL (i * 41) MINUTE) AS ts,
        (i % 6 = 0) AS is_ai,
        (i % 9 <> 0) AS has_user,
        (i % 13 <> 0 AND i % 4 <> 0) AS semantic_did_run,
        (i % 4) AS n_results
    FROM (SELECT i::INTEGER AS i FROM range(0, 200) t(i))
),
expanded AS (
    SELECT base.*, gs.pos
    FROM base, generate_series(1, base.n_results) AS gs(pos)
    WHERE base.n_results > 0
)
SELECT
    'sr' || i || '-' || pos AS search_result_key,
    CAST(ts AS DATE) AS partition_date,
    date_trunc('hour', ts) AS partition_hour,
    ts + INTERVAL 2 SECOND AS extracted_at,
    i AS search_id,
    ts AS searched_at,
    pos AS position,
    'item-' || ((i + pos) % 60) AS item_id,
    'Result ' || pos AS name,
    CASE WHEN pos % 3 = 0 THEN 'Series' WHEN pos % 3 = 1 THEN 'Episode' ELSE 'Movie' END AS type,
    2000 + (i % 25) AS year,
    CASE WHEN pos % 3 = 1 THEN 'Show ' || (i % 5) END AS series_name,
    CASE WHEN pos % 3 = 1 THEN 1 END AS season_number,
    CASE WHEN pos % 3 = 1 THEN pos END AS episode_number,
    (pos = 1 AND semantic_did_run) AS semantic,
    CASE WHEN pos = 1 AND semantic_did_run THEN 0.5 + (pos * 0.1) END AS semantic_score,
    CASE WHEN pos = 1 AND semantic_did_run THEN 1 END AS semantic_rank,
    CASE WHEN is_ai THEN 'Matches the mood you described.' END AS reason,
    CASE WHEN has_user THEN 'user-' || (i % 6) END AS user_id,
    CASE WHEN has_user THEN 'person' || (i % 6) END AS user_name,
    CASE WHEN i % 17 <> 0 THEN 'term ' || (i % 50) END AS search_term,
    CASE WHEN is_ai THEN 'ai' ELSE 'search' END AS kind
FROM expanded;
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


_AI_GATEWAY_CALLS = """
CREATE OR REPLACE TABLE marts.fct_ai_gateway_call AS
SELECT
    'call' || i AS call_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS created_at,
    date_trunc('hour', TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS created_hour,
    'app-' || (i % 3) AS application_id,
    'App ' || (i % 3) AS application_name,
    'key-' || (i % 4) AS api_key_id,
    'Key ' || (i % 4) AS key_label,
    CASE WHEN i % 5 = 0 THEN 'schedule' ELSE 'agent' END AS call_source,
    'workbench' AS client_name,
    '1.0' AS client_version,
    CASE WHEN i % 6 = 0 THEN 'owner' END AS actor_label,
    CAST(NULL AS VARCHAR) AS actor_external_id,
    'claude' AS requested_model,
    CASE WHEN i % 9 = 0 THEN 'claude-fallback' ELSE 'claude' END AS actual_model,
    (i % 9 = 0) AS was_rerouted,
    CASE WHEN i % 23 = 0 THEN 'failure' ELSE 'success' END AS status,
    CASE WHEN i % 23 = 0 THEN 'upstream_error' END AS failure_code,
    -- Zero tokens is a real, common state -- a streamed call the caller never asked
    -- usage figures for. i % 11 exercises it deliberately, not accidentally.
    CASE WHEN i % 11 = 0 THEN 0 ELSE 100 + i END AS input_tokens,
    CASE WHEN i % 11 = 0 THEN 0 ELSE 40 + (i % 30) END AS output_tokens,
    CASE WHEN i % 11 = 0 THEN 0 ELSE 140 + i + (i % 30) END AS total_tokens,
    CASE WHEN i % 11 = 0 THEN 0.0 ELSE (i % 7) * 0.001 END AS cost_eur,
    300 + (i % 50) * 10 AS latency_ms,
    CASE WHEN i % 4 = 0 THEN 'conv-' || (i % 20) END AS conversation_id,
    'prompt text ' || i AS prompt,
    'response text ' || i AS response,
    CAST(NULL AS VARCHAR) AS results,
    '{"clipUrl": null}' AS metadata,
    CASE WHEN i % 13 = 0 THEN (i % 4) ELSE 0 END AS frame_count,
    CASE WHEN i % 13 = 0 THEN (i % 4) * 40000 ELSE 0 END AS frame_bytes,
    CASE WHEN i % 13 = 0 THEN ['frames/' || i || '/0.jpg'] ELSE [] END AS frame_keys,
    (i % 13 = 0 AND i % 4 <> 0) AS has_frames
FROM (SELECT i::INTEGER AS i FROM range(0, 180) t(i));
"""

_AGENT_RUNS = """
CREATE OR REPLACE TABLE marts.fct_agent_run AS
SELECT
    'run' || i AS run_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS started_at,
    date_trunc('hour', TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS started_hour,
    CASE WHEN i % 17 <> 0 THEN TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE
                                + INTERVAL (5 + (i % 40)) SECOND END AS completed_at,
    CASE WHEN i % 17 <> 0 THEN (5 + (i % 40)) * 1000 END AS duration_ms,
    'conv-' || (i % 20) AS conversation_id,
    'Conversation ' || (i % 20) AS conversation_title,
    'agent-' || (i % 3) AS agent_id,
    'Agent ' || (i % 3) AS agent_name,
    'user-' || (i % 5) AS initiated_by_user_id,
    CASE WHEN i % 17 = 0 THEN 'awaiting_input'
         WHEN i % 19 = 0 THEN 'failed' ELSE 'completed' END AS status,
    'claude' AS requested_model,
    CASE WHEN i % 9 = 0 THEN 'claude-fallback' ELSE 'claude' END AS actual_route,
    (i % 9 = 0) AS was_rerouted,
    CASE WHEN i % 19 = 0 THEN 'tool_error' END AS failure_code,
    100 + i AS input_tokens,
    40 + (i % 30) AS output_tokens,
    140 + i + (i % 30) AS total_tokens,
    CASE WHEN i % 4 = 0 THEN 10 + (i % 20) END AS reasoning_tokens,
    (i % 7) * 0.002 AS cost_eur,
    CASE WHEN i % 17 = 0 THEN TIMESTAMPTZ '2026-09-02 06:00:00Z' END AS awaiting_input_deadline,
    (i % 17 = 0) AS is_awaiting_input,
    CASE WHEN i % 10 = 0 THEN 'report-def-1' END AS report_definition_id,
    (i % 10 = 0) AS is_report_run,
    '{}' AS metadata
FROM (SELECT i::INTEGER AS i FROM range(0, 160) t(i));
"""

_AGENT_TOOL_CALLS = """
CREATE OR REPLACE TABLE marts.fct_agent_tool_call AS
SELECT
    'step' || i AS tool_call_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS occurred_at,
    date_trunc('hour', TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS occurred_hour,
    'run' || (i % 160) AS run_id,
    (i % 6) AS sequence,
    CASE WHEN i % 4 = 0 THEN 'export_workbook'
         WHEN i % 4 = 1 THEN 'send_email'
         WHEN i % 4 = 2 THEN 'search_documents' ELSE 'run_query' END AS tool_name,
    CASE WHEN i % 4 = 2 THEN 'documents' WHEN i % 4 = 3 THEN 'datalake' END AS server_alias,
    '{"query": "x"}' AS arguments_json,
    CASE WHEN i % 15 <> 0 THEN '{"rows": 3}' END AS result_json,
    CASE WHEN i % 15 = 0 THEN 'failed' ELSE 'completed' END AS call_status,
    CASE WHEN i % 15 = 0 THEN 'timed out' END AS error_message,
    CASE WHEN i % 15 <> 0 THEN 200 + (i % 500) END AS duration_ms,
    'Step ' || i AS step_title,
    'Tool call step ' || i AS step_description,
    '{"tool_name": "x"}' AS payload
FROM (SELECT i::INTEGER AS i FROM range(0, 220) t(i));
"""

_CONVERSATION_MESSAGES = """
CREATE OR REPLACE TABLE marts.fct_conversation_message AS
SELECT
    'msg' || i AS message_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS created_at,
    date_trunc('hour', TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) MINUTE) AS created_hour,
    'conv-' || (i % 20) AS conversation_id,
    'Conversation ' || (i % 20) AS conversation_title,
    'agent-' || (i % 3) AS agent_id,
    'Agent ' || (i % 3) AS agent_name,
    'user-' || (i % 5) AS conversation_owner_user_id,
    CASE WHEN i % 2 = 0 THEN 'user-' || (i % 5) END AS author_user_id,
    CASE WHEN i % 2 <> 0 THEN 'run' || (i % 160) END AS run_id,
    CASE WHEN i % 2 = 0 THEN 'user' ELSE 'assistant' END AS role,
    'message content ' || i AS content,
    length('message content ' || i) AS content_length,
    '{}' AS metadata
FROM (SELECT i::INTEGER AS i FROM range(0, 300) t(i));
"""

_EMAIL_SENDS = """
CREATE OR REPLACE TABLE marts.fct_email_send AS
SELECT
    'email' || i AS email_send_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR) AS created_at,
    date_trunc('hour', TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR) AS created_hour,
    'user-' || (i % 5) AS owner_user_id,
    CASE WHEN i % 3 = 0 THEN 'run' || (i % 160) END AS run_id,
    CASE WHEN i % 6 = 0 THEN 'approval-' || i END AS approval_id,
    (i % 6 = 0) AS was_approved,
    CASE WHEN i % 5 = 0 THEN 'template-1' END AS template_script_id,
    CASE WHEN i % 5 = 0 THEN 2 END AS template_version,
    'Subject ' || i AS subject,
    '["a@example.test"]' AS recipients,
    1 + (i % 3) AS recipient_count,
    CASE WHEN i % 8 = 0 THEN '["report.xlsx"]' ELSE '[]' END AS attachments,
    CASE WHEN i % 8 = 0 THEN 1 ELSE 0 END AS attachment_count,
    (i % 8 = 0) AS has_attachments,
    CASE WHEN i % 12 = 0 THEN 'failed' ELSE 'sent' END AS status,
    (i % 12 <> 0) AS was_sent,
    CASE WHEN i % 12 = 0 THEN 'smtp_error' END AS failure_reason
FROM (SELECT i::INTEGER AS i FROM range(0, 90) t(i));
"""

_AUTOMATION_RUNS = """
CREATE OR REPLACE TABLE marts.fct_automation_run AS
SELECT
    'auto-run' || i AS automation_run_key,
    (DATE '2026-09-01' + (i % 14)) AS partition_date,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR) AS created_at,
    (TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR) AS started_at,
    CASE WHEN i % 10 <> 0 THEN TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR
                                + INTERVAL (2 + (i % 10)) MINUTE END AS finished_at,
    CASE WHEN i % 10 <> 0 THEN (2 + (i % 10)) * 60000 END AS duration_ms,
    'automation-' || (i % 4) AS automation_id,
    'Automation ' || (i % 4) AS automation_name,
    CASE WHEN i % 3 = 0 THEN 'dagster-run-' || i END AS dagster_run_id,
    CASE WHEN i % 10 = 0 THEN 'running'
         WHEN i % 14 = 0 THEN 'failed' ELSE 'succeeded' END AS status,
    3 + (i % 4) AS steps_total,
    CASE WHEN i % 14 = 0 THEN 1 + (i % 3) ELSE 3 + (i % 4) END AS steps_succeeded,
    CASE WHEN i % 14 = 0 THEN 1 ELSE 0 END AS steps_failed,
    CASE WHEN i % 10 = 0 THEN 1 ELSE 0 END AS steps_pending,
    (i % 14 = 0) AS had_a_failed_step,
    CASE WHEN i % 10 <> 0 THEN TIMESTAMPTZ '2026-09-01 06:00:00Z' + INTERVAL (i) HOUR
                                + INTERVAL (2 + (i % 10)) MINUTE END AS last_step_finished_at,
    '[{"step_id": "s1", "status": "succeeded"}]' AS step_runs_json
FROM (SELECT i::INTEGER AS i FROM range(0, 70) t(i));
"""


_FINANCE_TRANSACTIONS = """
CREATE OR REPLACE TABLE marts.fct_finance_transaction AS
SELECT
    'tx' || i AS transaction_key,
    (DATE '2026-09-01' + (i % 14)) AS booked_on,
    DATE '2026-09-01' AS booked_month,
    'acc-' || (i % 3) AS account_id,
    'Account ' || (i % 3) AS account_name,
    CASE WHEN i % 3 = 0 THEN 'CIC' ELSE 'Revolut' END AS bank,
    'Merchant ' || (i % 7) AS merchant,
    CASE WHEN i % 5 = 0 THEN 100.0 + i ELSE -(10.0 + i) END AS amount,
    'EUR' AS currency,
    CASE WHEN i % 5 = 0 THEN 100.0 + i ELSE -(10.0 + i) END AS amount_home,
    CASE WHEN i % 5 = 0 THEN 'inflow' ELSE 'outflow' END AS direction,
    CASE WHEN i % 4 = 0 THEN 'technology' ELSE 'groceries' END AS category,
    CASE WHEN i % 4 = 0 THEN 'Technology' ELSE 'Groceries' END AS category_label,
    CASE WHEN i % 4 = 0 THEN 'tech' ELSE 'daily' END AS category_group,
    NULL::VARCHAR AS transfer_category,
    CASE WHEN i % 5 = 0 THEN 'income' WHEN i % 4 = 0 THEN 'subscription'
         WHEN i % 9 = 0 THEN 'transfer' ELSE 'purchase' END AS transaction_type,
    'derived' AS type_source,
    CASE WHEN i % 9 = 0 THEN 'internal' END AS transfer_kind,
    CASE WHEN i % 4 = 0 THEN 'series-1' END AS recurring_series_id,
    (i % 11 = 0) AS needs_review,
    '[]' AS tags,
    NULL::VARCHAR AS notes,
    'CB MERCHANT ' || (i % 7) AS description,
    'posted' AS transaction_status,
    NULL::TIMESTAMPTZ AS posted_at,
    'synced' AS source,
    'ref-' || i AS source_transaction_id,
    '{}' AS metadata,
    '[]' AS tag_slugs,
    '[]' AS property_links,
    (i % 5 <> 0 AND i % 9 <> 0) AS is_spending,
    (i % 5 = 0 AND i % 9 <> 0) AS is_income
FROM (SELECT i::INTEGER AS i FROM range(0, 80) t(i));
"""

_FINANCE_BALANCES = """
CREATE OR REPLACE TABLE marts.fct_finance_account_balance AS
SELECT
    (DATE '2026-09-01' + (i % 14)) AS balance_date,
    'acc-' || (i % 3) AS account_id,
    'Account ' || (i % 3) AS account_name,
    CASE WHEN i % 3 = 0 THEN 'CIC' ELSE 'Revolut' END AS bank,
    'checking' AS account_type,
    NULL::VARCHAR AS role,
    'EUR' AS currency,
    1000.0 + i AS balance,
    'EUR' AS home_currency,
    1000.0 + i AS balance_home
FROM (SELECT i::INTEGER AS i FROM range(0, 42) t(i));
"""


# Documentation-range addresses only (tests/test_no_real_addresses_in_fixtures.py).
_EDGE_CLIENTS = """
CREATE OR REPLACE TABLE marts.dim_edge_client AS
SELECT
    '203.0.113.' || i AS client_address,
    '203.0.113.0/24' AS client_network,
    4 AS client_ip_version,
    CASE WHEN i % 3 = 0 THEN 'NL' ELSE 'US' END AS geo_country_code,
    CASE WHEN i % 3 = 0 THEN 'Netherlands' ELSE 'United States' END AS geo_country_name,
    'City ' || (i % 4) AS geo_city,
    52.0 AS geo_latitude,
    4.9 AS geo_longitude,
    64500 + (i % 3) AS geo_asn,
    'Example Cloud' AS geo_as_org,
    (i % 2 = 0) AS is_datacenter,
    false AS is_verified_crawler,
    (TIMESTAMPTZ '2026-09-01 00:00:00Z' + INTERVAL (i) HOUR) AS first_seen_at,
    (TIMESTAMPTZ '2026-09-02 00:00:00Z' + INTERVAL (i) HOUR) AS last_seen_at,
    CAST(TIMESTAMPTZ '2026-09-02 00:00:00Z' + INTERVAL (i) HOUR AS DATE) AS last_seen_date,
    1 + (i % 5) AS active_days,
    100 + i AS requests,
    i AS probe_requests,
    i AS client_errors,
    0 AS direct_requests,
    CASE WHEN i % 4 = 0 THEN 3 ELSE 0 END AS rejected_requests,
    10 + i AS peak_requests_per_minute,
    least(100, i * 3) AS max_threat_score,
    ['probe_paths', 'burst'] AS reasons,
    ['/.env'] AS sample_probe_paths,
    ['example.test'] AS sample_hosts,
    CASE WHEN i * 3 >= 70 THEN 'high' WHEN i * 3 >= 40 THEN 'medium' WHEN i > 0 THEN 'low' ELSE 'none' END AS threat_level,
    (i * 3 >= 40) AS is_flagged,
    CASE WHEN i % 4 = 0 THEN 'decision-' || i END AS decision_id,
    CASE WHEN i % 4 = 0 THEN 'block' END AS decision_action,
    CASE WHEN i % 4 = 0 THEN 'manual' END AS decision_rule,
    CASE WHEN i % 4 = 0 THEN '203.0.113.' || i || '/32' END AS decision_cidr,
    CASE WHEN i % 4 = 0 THEN 'scanner' END AS decision_reason,
    CASE WHEN i % 4 = 0 THEN 'admin' END AS decided_by,
    CASE WHEN i % 4 = 0 THEN TIMESTAMPTZ '2026-09-03 00:00:00Z' END AS decided_at,
    CAST(NULL AS TIMESTAMPTZ) AS decision_expires_at,
    (i % 4 = 0) AS is_blocked
FROM (SELECT i::INTEGER AS i FROM range(0, 40) t(i));
"""

_EDGE_DECISIONS = """
CREATE OR REPLACE TABLE marts.fct_edge_decision AS
SELECT
    'decision-' || i AS decision_id,
    (DATE '2026-09-01' + (i % 10)) AS partition_date,
    '203.0.113.' || i || '/32' AS cidr,
    '203.0.113.' || i AS network_address,
    32 AS prefix_length,
    4 AS ip_version,
    CASE WHEN i % 5 = 0 THEN 'allow' ELSE 'block' END AS action,
    CASE WHEN i % 2 = 0 THEN 'auto' ELSE 'manual' END AS decided_by_rule,
    'scanner' AS reason,
    CASE WHEN i % 2 = 0 THEN 90 END AS score,
    'admin' AS decided_by,
    (TIMESTAMPTZ '2026-09-01 00:00:00Z' + INTERVAL (i) HOUR) AS decided_at,
    CAST(NULL AS TIMESTAMPTZ) AS expires_at,
    CASE WHEN i % 3 = 0 THEN TIMESTAMPTZ '2026-09-12 00:00:00Z' END AS revoked_at,
    CASE WHEN i % 3 = 0 THEN 'admin' END AS revoked_by,
    CASE WHEN i % 3 = 0 THEN 'revoked' ELSE 'active' END AS status,
    (i % 3 <> 0) AS is_active
FROM (SELECT i::INTEGER AS i FROM range(0, 20) t(i));
"""

def build_marts(path: str) -> None:
    con = duckdb.connect(path)
    con.execute("SET TimeZone='UTC';")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    for statement in (
        _LLM, _HTTP, _CAMERA, _LIBRARY, _JELLYFIN_PLAYBACK, _JELLYFIN_ACTIVITY,
        _JELLYFIN_SEARCHES, _JELLYFIN_SEARCH_RESULTS,
        _AI_GATEWAY_CALLS, _AGENT_RUNS, _AGENT_TOOL_CALLS, _CONVERSATION_MESSAGES,
        _EMAIL_SENDS, _AUTOMATION_RUNS, _FINANCE_TRANSACTIONS, _FINANCE_BALANCES,
        _EDGE_CLIENTS, _EDGE_DECISIONS,
    ):
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
