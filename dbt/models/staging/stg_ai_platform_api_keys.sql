-- The API key registry, current state only. Same reasoning as stg_ai_platform_applications:
-- a whole-table snapshot landed daily, deduped to the newest reading per key so a join
-- gets "this key's label today" rather than every day it has ever existed.

with source as (

    select * from {{ source('raw', 'platform_api_keys') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id,
    application_id,
    created_by_user_id,
    nullif(label, '')      as key_label,
    prefix,
    suffix,
    nullif(status, '')     as status,
    last_used_at,
    revoked_at,
    created_at,
    updated_at

from source
