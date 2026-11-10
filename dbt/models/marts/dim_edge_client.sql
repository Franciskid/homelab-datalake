-- One row per public client address seen by NPM in the last 90 days: the evidence
-- against it over the whole window, and the owner's current decision about it.
--
-- The one place to ask "which flagged addresses are blocked, which are not, and
-- why". Built from agg_edge_client_daily (the per-day scores) and fct_edge_decision.
--
-- Which decision applies follows the platform's own rule: a "never block" (allow)
-- wins over a block, and among several the most specific range wins. IPv4 ranges are
-- matched arithmetically; IPv6 decisions are matched on the exact address, which is
-- the only kind the report creates for IPv6.

with clients as (

    select
        client_address,
        any_value(client_network)                               as client_network,
        any_value(client_ip_version)                            as client_ip_version,
        any_value(geo_country_code)                             as geo_country_code,
        any_value(geo_country_name)                             as geo_country_name,
        any_value(geo_city)                                     as geo_city,
        any_value(geo_latitude)                                 as geo_latitude,
        any_value(geo_longitude)                                as geo_longitude,
        any_value(geo_asn)                                      as geo_asn,
        any_value(geo_as_org)                                   as geo_as_org,
        bool_or(is_datacenter)                                  as is_datacenter,
        bool_or(is_verified_crawler)                            as is_verified_crawler,
        min(first_seen_at)                                      as first_seen_at,
        max(last_seen_at)                                       as last_seen_at,
        count(*)                                                as active_days,
        sum(requests)                                           as requests,
        sum(probe_requests)                                     as probe_requests,
        sum(client_errors)                                      as client_errors,
        sum(direct_requests)                                    as direct_requests,
        sum(rejected_requests)                                  as rejected_requests,
        max(peak_requests_per_minute)                           as peak_requests_per_minute,
        max(threat_score)                                       as max_threat_score,
        list_distinct(flatten(list(reasons)))                   as reasons,
        list_distinct(flatten(list(sample_probe_paths)))[1:8]   as sample_probe_paths,
        list_distinct(flatten(list(sample_hosts)))[1:8]         as sample_hosts
    from {{ ref('agg_edge_client_daily') }}
    group by client_address

),

ipv4 as (

    select
        client_address,
        case when client_address like '%.%.%.%' and client_address not like '%:%' then
            (try_cast(split_part(client_address, '.', 1) as bigint) << 24)
          + (try_cast(split_part(client_address, '.', 2) as bigint) << 16)
          + (try_cast(split_part(client_address, '.', 3) as bigint) << 8)
          +  try_cast(split_part(client_address, '.', 4) as bigint)
        end                                                     as address_int
    from clients

),

decisions as (

    select
        *,
        case when ip_version = 4 then
            (try_cast(split_part(network_address, '.', 1) as bigint) << 24)
          + (try_cast(split_part(network_address, '.', 2) as bigint) << 16)
          + (try_cast(split_part(network_address, '.', 3) as bigint) << 8)
          +  try_cast(split_part(network_address, '.', 4) as bigint)
        end                                                     as network_int
    from {{ ref('fct_edge_decision') }}
    where is_active

),

matched as (

    select
        i.client_address,
        d.*,
        row_number() over (
            partition by i.client_address
            order by (d.action = 'allow') desc, d.prefix_length desc, d.decided_at desc
        )                                                       as rank
    from ipv4 i
    join decisions d
      on (d.ip_version = 4 and i.address_int is not null and d.prefix_length between 0 and 32
          and (i.address_int >> (32 - d.prefix_length)) = (d.network_int >> (32 - d.prefix_length)))
      or (d.ip_version = 6 and d.prefix_length = 128 and d.network_address = lower(i.client_address))

)

select
    c.*,
    case
        when c.max_threat_score >= 70 then 'high'
        when c.max_threat_score >= 40 then 'medium'
        when c.max_threat_score > 0 then 'low'
        else 'none'
    end                                                         as threat_level,
    c.max_threat_score >= 40                                    as is_flagged,
    m.decision_id,
    m.action                                                    as decision_action,
    m.decided_by_rule                                           as decision_rule,
    m.cidr                                                      as decision_cidr,
    m.reason                                                    as decision_reason,
    m.decided_by,
    m.decided_at,
    m.expires_at                                                as decision_expires_at,
    coalesce(m.action = 'block', false)                         as is_blocked

from clients c
left join matched m
  on m.client_address = c.client_address and m.rank = 1
