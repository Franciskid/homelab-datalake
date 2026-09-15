-- The account each play and activity entry points at. Latest snapshot only --
-- accounts do not carry the kind of history dim_title_scd2 exists for, so this is a
-- plain current-state lookup, the same shape as dim_camera_zone.

with latest as (

    select
        *,
        row_number() over (partition by user_id order by snapshot_date desc) as recency
    from {{ ref('stg_jellyfin_users') }}

)

select
    user_id,
    user_name,
    is_administrator,
    is_disabled,
    last_login,
    last_activity,
    snapshot_date                                        as last_snapshot_date
from latest
where recency = 1
