-- The request-grain fact table. One row per HTTP request.
--
-- Kept at full grain rather than pre-aggregated: at ~34 MB/day this is small enough
-- that a rollup would buy nothing and would foreclose questions nobody has asked yet.
-- The daily aggregate below is built from this, not instead of it.
--
-- `client_address` and `query_string` carry through from staging unchanged: raw,
-- private-warehouse-only, alongside the request_key/client_hash/client_network they
-- sit beside. See homelab_datalake.privacy for the 2026-09-14 decision.

select
    -- Every field the log records about a request, so two rows share a key exactly when
    -- nothing recorded tells them apart. Until 19 Sep 2026 this was six fields, and it
    -- merged requests the log *could* tell apart: 718k rows of Keycloak's OIDC endpoint
    -- differing only in their `state` and `nonce`, plus smaller sets differing in
    -- method, user agent or host. query_string arrived in this table on 14 Sep and was
    -- never added here -- assert_request_key_stays_discriminating now fails the build
    -- on exactly that drift, for any column.
    --
    -- A JSON array rather than a '|'-joined string: the old form was ambiguous once a
    -- field could contain the separator, and query strings and user agents can. Nulls
    -- serialise as null, distinct from an empty string.
    md5(to_json([
        npm_instance,
        cast(requested_at as varchar),
        client_hash,
        request_host,
        scheme,
        method,
        path,
        query_string,
        cast(status as varchar),
        upstream_status,
        upstream_target,
        cast(bytes_sent as varchar),
        user_agent
    ]))                                 as request_key,

    partition_date,
    npm_instance,
    requested_at,
    date_trunc('hour', requested_at)    as requested_hour,

    request_host,
    path,
    path_group,
    query_string,
    method,
    scheme,
    status,
    status_class,
    upstream_status,
    upstream_target,
    bytes_sent,
    user_agent,

    client_address,
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
