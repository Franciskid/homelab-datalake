-- Every conversation, typed. One row per conversation for the whole time it exists --
-- see ai_platform_registry.py: `conversations` is MODE_WINDOW on updated_at, so a row
-- lands under the day it was last touched and stays there; it is not re-landed under
-- every later day the way a MODE_SNAPSHOT table would be.

with source as (

    select * from {{ source('raw', 'platform_conversations') }}

)

select
    dt                                          as partition_date,
    id                                           as conversation_id,
    owner_user_id,
    team_id,
    nullif(title, '')                           as title,
    agent_id,
    created_at,
    updated_at,
    deleted_at,
    (deleted_at is not null)                    as is_deleted,
    nullif(reasoning_level, '')                 as reasoning_level,
    nullif(origin, '')                          as origin

from source
