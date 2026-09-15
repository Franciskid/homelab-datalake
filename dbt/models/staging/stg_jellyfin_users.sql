-- Account snapshots, typed. Name and two flags -- nothing from configuration or
-- password policy beyond those, by the owner's own instruction.

select
    dt                                              as snapshot_date,
    nullif(id, '')                                   as user_id,
    nullif(name, '')                                  as user_name,
    coalesce(is_administrator, false)                 as is_administrator,
    coalesce(is_disabled, false)                      as is_disabled,
    last_login,
    last_activity,
    nullif(_unmapped, '')                             as unmapped_json
from {{ source('raw', 'jellyfin_users') }}
