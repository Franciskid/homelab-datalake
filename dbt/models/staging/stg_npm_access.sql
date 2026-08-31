-- Typed, cleaned request rows.
--
-- Two judgement calls worth knowing about:
--
-- `request_host` is the Host header, which the client controls. Scanners send
-- things like "192.000.002.105", "127.0.0.1" and "192.0.2.1", all of which appear
-- in the real logs. It is kept as-is here and flagged, rather than cleaned, so the
-- mart can decide -- a scanner hitting a bogus Host is a fact worth counting, not noise
-- to launder.
--
-- `is_internal` uses the redacted private-address flag rather than re-deriving it,
-- because the address itself is gone by this point. That is the boundary working.

with source as (

    select * from {{ source('raw', 'npm_access') }}

),

renamed as (

    select
        dt                                          as partition_date,
        host_source                                 as npm_instance,
        timestamp                                   as requested_at,
        log_format,

        nullif(trim(request_host), '')              as request_host,
        upper(nullif(trim(method), ''))             as method,
        lower(nullif(trim(scheme), ''))             as scheme,
        nullif(path, '')                            as path,

        status,
        nullif(upstream_status, '')                 as upstream_status,
        nullif(cache_status, '')                    as cache_status,
        coalesce(bytes_sent, 0)                     as bytes_sent,
        nullif(user_agent, '')                      as user_agent,
        nullif(sent_to, '')                         as upstream_target,

        client_hash,
        client_network,
        client_trust,
        client_version                              as client_ip_version,
        coalesce(client_is_private, false)          as is_internal,

        geo_country_code,
        geo_country_name,
        geo_city,
        geo_asn,
        geo_as_org,
        source_file,
        referer,
        gzip_ratio,
        has_query,
        query_param_names

    from source

)

select
    *,
    case
        when status >= 500 then 'server_error'
        when status >= 400 then 'client_error'
        when status >= 300 then 'redirect'
        when status >= 200 then 'success'
        else 'informational'
    end                                             as status_class,

    -- A Host header that is an address rather than a name is a scanner probing the
    -- IP directly. Worth naming, because it is most of the 4xx traffic.
    (request_host is not null
     and regexp_matches(request_host, '^[0-9a-fA-F:.]+$'))    as host_is_address

from renamed
