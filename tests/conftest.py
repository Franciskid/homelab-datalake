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
    500 + (i % 50) * 20 AS duration_ms, 120 + (i % 30) AS time_to_first_token_ms,
    's' || (i % 9) AS session_id
FROM (SELECT i::INTEGER AS i FROM range(0, 200) t(i));
"""

_HTTP = """
CREATE OR REPLACE TABLE marts.fct_http_request AS
SELECT
    'r' || i AS request_key,
    (DATE '2026-08-01' + (i % 20)) AS partition_date,
    'primary' AS npm_instance,
    CASE WHEN i % 4 = 0 THEN 'tv.example.org' ELSE 'data.example.org' END AS request_host,
    '/api' AS path_group, 'GET' AS method, 'https' AS scheme,
    CASE WHEN i % 23 = 0 THEN 500 ELSE 200 END AS status,
    CASE WHEN i % 23 = 0 THEN 'server_error' ELSE 'success' END AS status_class,
    CASE WHEN i % 6 = 0 THEN 'bot' ELSE 'browser' END AS client_kind,
    CASE WHEN i % 5 = 0 THEN 'cdn_edge' ELSE 'resolved' END AS client_trust,
    4 AS client_ip_version, false AS is_internal, (i % 6 = 0) AS is_automated,
    'h' || (i % 40) AS client_hash, 'n' || (i % 12) AS client_network,
    'FR' AS geo_country_code, 'France' AS geo_country_name, 'Paris' AS geo_city,
    'Example AS' AS geo_as_org, 1000 + i AS bytes_sent
FROM (SELECT i::INTEGER AS i FROM range(0, 300) t(i));
"""

_CAMERA = """
CREATE OR REPLACE TABLE marts.fct_camera_event AS
SELECT
    'e' || i AS event_id,
    (DATE '2026-08-01' + (i % 20)) AS partition_date,
    'cam-' || (i % 2) AS camera_id, 'zone-' || (i % 3) AS zone_id,
    CASE WHEN i % 3 = 0 THEN 'driveway'
         WHEN i % 3 = 1 THEN 'garden' ELSE 'door' END AS zone_name,
    'presence' AS rule_type,
    CASE WHEN i % 10 = 0 THEN 'candidate'
         WHEN i % 3 = 0 THEN 'confirmed' ELSE 'dismissed' END AS lifecycle,
    (i % 3 = 0 AND i % 10 <> 0) AS is_confirmed,
    (i % 10 = 0) AS is_awaiting_analysis,
    CAST(NULL AS VARCHAR) AS direction, 'occupied' AS zone_state,
    CASE WHEN i % 3 <> 0 THEN 'semantic_no_event' END AS dismissal_reason,
    'Monday' AS started_weekday, (i % 24) AS started_hour_of_day, (i % 7 > 4) AS is_weekend,
    (i % 11 = 0) AS is_unusual, (i % 4 = 0) AS should_notify, (i % 31 = 0) AS is_duplicate,
    CASE WHEN i % 10 <> 0 THEN (i % 100) / 100.0 END AS confidence,
    10 + (i % 40) AS duration_seconds
FROM (SELECT i::INTEGER AS i FROM range(0, 250) t(i));
"""

# Three snapshot dates on purpose: the rule that a range selects one snapshot rather
# than summing across them is only testable when there is more than one.
_LIBRARY = """
CREATE OR REPLACE TABLE marts.fct_library_title AS
SELECT
    'k' || i || '-' || d AS title_key, 'item' || i AS item_id,
    (DATE '2026-08-25' + d) AS snapshot_date,
    'Film ' || i AS title, 1990 + (i % 30) AS production_year,
    ((1990 + (i % 30)) / 10) * 10 AS decade,
    CASE WHEN i % 4 = 0 THEN '4K'
         WHEN i % 4 = 1 THEN '1080p' ELSE '720p' END AS resolution_class,
    'h264' AS video_codec, 'aac' AS audio_codec, 'mkv' AS container,
    'PG' AS official_rating, 'Studio ' || (i % 5) AS studio,
    CASE WHEN i % 50 = 0 THEN 1700 ELSE 90 + (i % 60) END AS runtime_minutes,
    (i % 50 <> 0) AS is_runtime_plausible, (i % 3 = 0) AS is_long_feature,
    (i % 2 = 0) AS is_high_bit_depth, (i % 20 <> 0) AS is_enrichable,
    CASE WHEN i % 6 <> 0 THEN 5.0 + (i % 50) / 10.0 END AS community_rating,
    -- TMDB enrichment, null for some rows on purpose: unenriched titles are a
    -- supported state, not a broken one, and the measures have to survive them.
    CASE WHEN i % 7 <> 0 THEN 'en' END AS original_language,
    CASE WHEN i % 7 <> 0 THEN 'US' END AS production_country,
    CASE WHEN i % 7 <> 0 THEN DATE '2005-01-01' + i END AS tmdb_release_date,
    CASE WHEN i % 9 = 0 THEN 20000000 END AS budget_usd,
    CASE WHEN i % 9 = 0 THEN 55000000 END AS revenue_usd,
    CASE WHEN i % 9 = 0 THEN 2.75 END AS revenue_to_budget,
    CASE WHEN i % 7 <> 0 THEN (i % 90) / 3.0 END AS tmdb_popularity,
    CASE WHEN i % 7 <> 0 THEN 4.0 + (i % 60) / 10.0 END AS tmdb_vote_average,
    CASE WHEN i % 7 <> 0 THEN 100 + i END AS tmdb_vote_count,
    (i % 7 <> 0) AS is_enriched
FROM (SELECT i::INTEGER AS i FROM range(0, 120) t(i)),
     (SELECT d::INTEGER AS d FROM range(0, 3) s(d));
"""


def build_marts(path: str) -> None:
    con = duckdb.connect(path)
    con.execute("SET TimeZone='UTC';")
    con.execute("CREATE SCHEMA IF NOT EXISTS marts;")
    for statement in (_LLM, _HTTP, _CAMERA, _LIBRARY):
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
