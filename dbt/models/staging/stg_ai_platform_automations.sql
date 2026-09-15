-- The automation registry, current state only -- same dedup reasoning as
-- stg_ai_platform_applications.

with source as (

    select * from {{ source('raw', 'platform_automations') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id,
    owner_user_id,
    nullif(name, '')               as automation_name,
    nullif(description, '')        as automation_description,
    enabled,
    cadence_cron,
    timezone,
    consecutive_failures,
    paused_reason,
    paused_at,
    last_error,
    created_at,
    updated_at

from source
