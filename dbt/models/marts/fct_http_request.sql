-- The request-grain fact table. One row per HTTP request.
--
-- Kept at full grain rather than pre-aggregated: at ~[volume withheld]/day this is small enough
-- that a rollup would buy nothing and would foreclose questions nobody has asked yet.
-- The daily aggregate below is built from this, not instead of it.

select
    md5(
        npm_instance || '|' || cast(requested_at as varchar) || '|' ||
        coalesce(client_hash, '') || '|' || coalesce(path, '') || '|' ||
        cast(status as varchar) || '|' || cast(bytes_sent as varchar)
    )                                   as request_key,

    partition_date,
    npm_instance,
    requested_at,
    date_trunc('hour', requested_at)    as requested_hour,

    request_host,
    path,
    path_group,
    method,
    scheme,
    status,
    status_class,
    upstream_status,
    upstream_target,
    bytes_sent,
    user_agent,

    client_hash,
    client_network,
    client_trust,
    client_ip_version,
    is_internal,
    client_kind,
    is_automated,

    geo_country_code,
    geo_country_name,
    geo_city,
    geo_asn,
    geo_as_org,

    log_format

from {{ ref('int_npm_requests') }}
