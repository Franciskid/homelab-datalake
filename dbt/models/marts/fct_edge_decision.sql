-- Every block and "never block" decision the owner (or the auto-block rule they
-- agreed to) took on the Edge security report, lifted ones included.
--
-- `status` is computed at build time: a decision whose expiry passed since the last
-- rebuild still reads `active` until the next hourly run. The NPM blocklist itself
-- is applied from the platform directly, never from here.

select
    decision_id,
    cast(decided_at as date)                            as partition_date,
    cidr,
    network_address,
    prefix_length,
    ip_version,
    action,
    decided_by_rule,
    reason,
    score,
    decided_by,
    decided_at,
    expires_at,
    revoked_at,
    revoked_by,
    case
        when revoked_at is not null then 'revoked'
        when expires_at is not null and expires_at <= now() then 'expired'
        else 'active'
    end                                                 as status,
    (revoked_at is null and (expires_at is null or expires_at > now())) as is_active

from {{ ref('stg_ai_platform_edge_blocks') }}
