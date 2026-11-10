-- Owner decisions about addresses at the NPM edge, current state only.
--
-- The platform never deletes a decision; lifting one sets revoked_at. So the latest
-- snapshot of a row is its current state, and the table is a complete history.

with source as (

    select * from {{ source('raw', 'platform_edge_blocks') }}
    qualify row_number() over (partition by id order by dt desc) = 1

)

select
    id                                                  as decision_id,
    lower(cidr)                                         as cidr,
    lower(split_part(cidr, '/', 1))                     as network_address,
    try_cast(split_part(cidr, '/', 2) as integer)       as prefix_length,
    case when cidr like '%:%' then 6 else 4 end         as ip_version,
    action,
    source                                              as decided_by_rule,
    nullif(reason, '')                                  as reason,
    score,
    nullif(created_by_subject, '')                      as decided_by,
    created_at                                          as decided_at,
    expires_at,
    revoked_at,
    nullif(revoked_by_subject, '')                      as revoked_by

from source
