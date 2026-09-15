-- The agent registry, current state only -- same dedup reasoning as
-- stg_ai_platform_applications.

with source as (

    select * from {{ source('raw', 'platform_agents') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id,
    owner_user_id,
    team_id,
    nullif(name, '')               as agent_name,
    nullif(description, '')        as agent_description,
    nullif(status, '')             as status,
    current_version,
    created_at,
    updated_at,
    template_id

from source
