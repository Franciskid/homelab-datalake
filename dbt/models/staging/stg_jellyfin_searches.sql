-- Search requests, typed. One row per request the AI Search plugin logged: the
-- server's own /Items, /Search/Hints, /Persons and /Artists searches, and the
-- plugin's own AI popup searches (kind = 'ai').
--
-- searched_at, client_hash, client_network and client_trust are already correct
-- here, not derived: the raw asset parses the plugin's own timestamp and redacts
-- the client address at ingest, the same way stg_jellyfin_playback's timestamps
-- and stg_npm_access's client columns are already correct by the time they reach
-- staging. Nothing here repeats that work.

with source as (select * from {{ source('raw', 'jellyfin_searches') }}),

renamed as (
    select
        dt                                          as partition_date,
        partition_hour,
        _extracted_at                                as extracted_at,
        id                                            as search_id,
        searched_at,
        nullif(kind, '')                              as kind,
        nullif(endpoint, '')                          as endpoint,
        nullif(method, '')                            as method,
        status_code,
        duration_ms,
        nullif(search_term, '')                       as search_term,
        nullif(user_id, '')                           as user_id,
        nullif(user_name, '')                         as user_name,
        is_api_key,
        nullif(client, '')                            as client,
        nullif(client_version, '')                    as client_version,
        nullif(device, '')                            as device,
        nullif(device_id, '')                         as device_id,
        nullif(remote_address, '')                    as remote_address,
        nullif(client_hash, '')                       as client_hash,
        nullif(client_network, '')                    as client_network,
        nullif(client_trust, '')                      as client_trust,
        nullif(user_agent, '')                        as user_agent,
        nullif(parameters, '')                        as parameters_json,
        nullif(parent_id, '')                         as parent_id,
        nullif(parent_name, '')                       as parent_name,
        result_count,
        total_record_count,
        results_logged,
        semantic_enabled,
        semantic_ran,
        nullif(semantic_skipped_reason, '')            as semantic_skipped_reason,
        nullif(semantic_mode, '')                      as semantic_mode,
        semantic_duration_ms,
        semantic_raw_hits,
        semantic_matches,
        nullif(semantic_error, '')                     as semantic_error,
        nullif(mode, '')                               as mode,
        nullif(answer, '')                             as answer,
        nullif(_unmapped, '')                          as unmapped_json
    from source
)

select
    *,
    (kind = 'ai')                                     as is_ai_search,
    (result_count = 0)                                as had_no_results
from renamed
