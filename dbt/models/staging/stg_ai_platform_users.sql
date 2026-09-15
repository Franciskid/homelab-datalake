-- The user registry, current state only -- same dedup reasoning as
-- stg_ai_platform_applications. A handful of rows: this is a household's own
-- platform, not a multi-tenant service.

with source as (

    select * from {{ source('raw', 'platform_users') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id,
    nullif(username, '')       as username,
    nullif(display_name, '')   as display_name,
    nullif(status, '')         as status,
    first_seen_at,
    last_seen_at,
    last_login_at

from source
