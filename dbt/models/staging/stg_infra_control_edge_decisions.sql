-- Edge decisions from fraserv-infra's infra-control, current state only.
--
-- Each day's partition is a full snapshot and nothing is ever deleted upstream (lifting
-- a decision sets revoked_at), so the newest snapshot holds every decision as it is now.

with source as (

    select * from {{ source('raw', 'infra_control_edge_decisions') }}
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
    nullif(created_by, '')                              as decided_by,
    created_at                                          as decided_at,
    expires_at,
    revoked_at,
    nullif(revoked_by, '')                              as revoked_by,
    nullif(via, '')                                     as decided_via

from source
