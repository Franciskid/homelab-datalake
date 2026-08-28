-- Daily traffic per instance, host and audience type.
--
-- `visitors` counts only rows whose client address is a real client. Before the
-- 27 Aug 2026 real-IP cutover, Cloudflare-proxied requests logged the edge, so
-- counting distinct hashes across the whole history would average real people with
-- a handful of CDN machines and report a confidently wrong number. Excluding
-- `cdn_edge` makes the metric mean one thing for its whole range.

select
    partition_date,
    npm_instance,
    request_host,
    client_kind,

    count(*)                                                as requests,
    count(distinct client_hash)
        filter (where client_trust = 'resolved')            as visitors,
    count(distinct client_network)
        filter (where client_trust = 'resolved')            as visitor_networks,
    count(*) filter (where client_trust = 'cdn_edge')       as requests_behind_cdn,

    sum(bytes_sent)                                         as bytes_sent,
    count(*) filter (where status_class = 'success')        as requests_success,
    count(*) filter (where status_class = 'client_error')   as requests_client_error,
    count(*) filter (where status_class = 'server_error')   as requests_server_error,
    round(100.0 * count(*) filter (where status_class = 'server_error')
          / nullif(count(*), 0), 3)                         as server_error_pct,

    count(distinct geo_country_code)                        as countries

from {{ ref('fct_http_request') }}
group by 1, 2, 3, 4
