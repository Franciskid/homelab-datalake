-- The application registry, current state only.
--
-- `platform_applications` is a whole-table snapshot landed every day (see
-- ai_platform_registry.py: MODE_SNAPSHOT), so the raw table holds one copy of every
-- application for every day since it was first seen -- a real history, and useful
-- for its own sake, but not what a join wants. `qualify` keeps the newest `dt` per
-- id, which is what "the application's name today" means to fct_ai_gateway_call.

with source as (

    select * from {{ source('raw', 'platform_applications') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id,
    created_by_user_id,
    team_id,
    nullif(name, '')               as application_name,
    nullif(environment, '')        as environment,
    nullif(status, '')             as status,
    monthly_budget_eur,
    created_at,
    updated_at,
    last_used_at,
    parent_application_id

from source
