-- One row per public client address per day, with the evidence that it is probing
-- the edge and a 0-100 threat score built from that evidence.
--
-- This is what the edge-threat report on ai.fraserv.fr reads, and what its optional
-- auto-block acts on, so every point of the score is a named, countable fact rather
-- than a model's opinion: the `reasons` column lists exactly which rules fired.
--
-- What the rules are built on, measured over 30 days of real traffic (Nov 2026):
--
--   * Probe paths. ~1,000 addresses asked for at least one path no application here
--     serves and every scanner asks for: /.env and its variants, /.git/, phpunit's
--     eval-stdin, /proc/self/environ through traversal, cgi-bin, router admin pages.
--     A single hit is weak evidence (a mistyped URL exists), five is not.
--   * Bursts. The scanners that triggered the backend's alert emails sent 20-100
--     requests in one to three seconds, rotating user agents between them.
--   * Datacenter origin. Nearly all of them came from cloud ASNs (Google Cloud in
--     particular), where people do not browse from.
--   * Bypassing Cloudflare. A Host header that is an IP, no Host at all, or a peer
--     nginx labels `direct` on a Cloudflare-only host can only come from someone who
--     found the origin address.
--
-- What it deliberately does not do:
--
--   * Flag verified search crawlers. Googlebot from Google's own ASN and Bingbot from
--     Microsoft's are scored 0 whatever they request: blocking them costs search
--     ranking, and they follow links rather than wordlists.
--   * Treat `/config.json` as a probe on its own: Jellyfin's web client loads
--     /web/config.json on every visit. Only dot-directory configs (/.continue/,
--     /.anthropic/, ...) count.
--   * Look further back than 90 days. The report shows at most 30, and the window
--     function behind peak-per-minute is the expensive part of this model.
--
-- Raw client addresses, private-warehouse-only, the same 2026-09-14 decision as
-- fct_http_request. Not registered for lake export.

{% set probe_pattern =
    "(/\\.env|/\\.git/|/\\.aws|/\\.ssh|/\\.svn|/\\.ds_store|/\\.vscode|/\\.idea|/\\.docker/"
    ~ "|/\\.[a-z0-9_-]+/(config|settings|credentials)"
    ~ "|wp-(login|admin|content|includes|config)|xmlrpc\\.php|phpmyadmin|/pma/"
    ~ "|/proc/self/|\\.\\./|%2e%2e|%252e|\\.\\.;"
    ~ "|/cgi-bin/|/vendor/phpunit|/boaform|/hnap1|/owa/|/actuator|/server-status"
    ~ "|/solr/|/druid/|/telescope|/_profiler|/console/api_server|/geoserver|/manager/html"
    ~ "|/remote/login|/sslvpn|/dana-na|/ecp/|/mifs/|/setup\\.cgi"
    ~ "|/api/fs/(read|exec)|/api/inngest|/api/v1/validate/code|/api/templates/preview"
    ~ "|/api/designer/|/api/system/fileview|/sysconfig/getall|/jobs_u/get_log_file"
    ~ "|sftp-config\\.json|docker-compose\\.ya?ml|/assets/environments?/"
    ~ "|\\.(sql|bak|old|swp)$|/shell(\\.php)?$)"
%}

{% set datacenter_pattern =
    "(google|amazon|aws|microsoft|azure|digitalocean|ovh|hetzner|linode|akamai|alibaba"
    ~ "|tencent|contabo|vultr|choopa|m247|leaseweb|scaleway|oracle|hostinger|ionos"
    ~ "|datacamp|cdn77|psychz|hostroyale|serverion|pfcloud|tzulo|colocrossing|frantech)"
%}

{% set self_addresses = env_var('EDGE_SELF_ADDRESSES', '').split(',') | map('trim') | reject('equalto', '') | list %}

with requests as (

    select
        *,
        lower(coalesce(path, '') || coalesce('?' || query_string, '')) as target,
        count(*) over (
            partition by client_address, date_trunc('minute', requested_at)
        )                                                               as same_minute
    from {{ ref('int_npm_requests') }}
    where partition_date >= current_date - 90
      and client_address is not null
      and not is_internal
      and coalesce(edge_route, '') <> 'lan'
      {% if self_addresses %}
      and client_address not in ({% for a in self_addresses %}'{{ a }}'{% if not loop.last %}, {% endif %}{% endfor %})
      {% endif %}

),

flagged as (

    select
        *,
        regexp_matches(target, '{{ probe_pattern }}')                   as is_probe,
        (request_host is null or host_is_address)                        as is_address_host
    from requests

),

daily as (

    select
        partition_date                                                  as activity_date,
        client_address,
        any_value(client_network)                                       as client_network,
        any_value(client_ip_version)                                    as client_ip_version,
        any_value(geo_country_code)                                     as geo_country_code,
        any_value(geo_country_name)                                     as geo_country_name,
        any_value(geo_city)                                             as geo_city,
        any_value(geo_latitude)                                         as geo_latitude,
        any_value(geo_longitude)                                        as geo_longitude,
        any_value(geo_asn)                                              as geo_asn,
        any_value(geo_as_org)                                           as geo_as_org,

        count(*)                                                        as requests,
        count(*) filter (where is_probe)                                as probe_requests,
        count(*) filter (where status between 400 and 499)              as client_errors,
        count(*) filter (where status >= 500)                           as server_errors,
        -- 444 is what NPM answers a blocked or Cloudflare-bypassing request with.
        count(*) filter (where status = 444)                            as rejected_requests,
        count(*) filter (where edge_route = 'direct')                   as direct_requests,
        count(*) filter (where edge_route = 'cf')                       as cloudflare_requests,
        count(*) filter (where is_address_host)                         as address_host_requests,
        count(distinct path)                                            as distinct_paths,
        count(distinct request_host)                                    as distinct_hosts,
        count(distinct user_agent)                                      as distinct_user_agents,
        max(same_minute)                                                as peak_requests_per_minute,
        min(requested_at)                                               as first_seen_at,
        max(requested_at)                                               as last_seen_at,

        list_distinct(list(path) filter (where is_probe))[1:8]          as sample_probe_paths,
        list_distinct(list(coalesce(request_host, '(no host)')))[1:8]   as sample_hosts,
        list_distinct(list(user_agent) filter (where user_agent is not null))[1:4]
                                                                        as sample_user_agents,

        coalesce(bool_or(
            (regexp_matches(lower(coalesce(user_agent, '')), 'googlebot') and geo_asn = 15169)
            or (regexp_matches(lower(coalesce(user_agent, '')), 'bingbot') and geo_asn = 8075)
        ), false)                                                       as is_verified_crawler

    from flagged
    group by 1, 2

),

scored as (

    select
        *,
        coalesce(regexp_matches(lower(coalesce(geo_as_org, '')), '{{ datacenter_pattern }}'), false)
                                                                        as is_datacenter,
        case when requests > 0 then client_errors::double / requests end
                                                                        as client_error_ratio
    from daily

),

rules as (

    select
        *,
        case when probe_requests > 0 then least(55, 25 + 6 * probe_requests) else 0 end
                                                                        as pts_probe,
        case when requests >= 10 and client_error_ratio >= 0.5 then 15 else 0 end
                                                                        as pts_errors,
        case when distinct_paths >= 25 then 10 else 0 end               as pts_breadth,
        case when peak_requests_per_minute >= 60 then 10 else 0 end     as pts_burst,
        case when is_datacenter then 10 else 0 end                      as pts_datacenter,
        case when direct_requests > 0 or address_host_requests > 0 then 15 else 0 end
                                                                        as pts_origin,
        case when distinct_user_agents >= 4 then 10 else 0 end          as pts_ua_rotation
    from scored

)

select
    activity_date,
    client_address,
    client_network,
    client_ip_version,
    geo_country_code,
    geo_country_name,
    geo_city,
    geo_latitude,
    geo_longitude,
    geo_asn,
    geo_as_org,
    is_datacenter,
    is_verified_crawler,

    requests,
    probe_requests,
    client_errors,
    server_errors,
    rejected_requests,
    direct_requests,
    cloudflare_requests,
    address_host_requests,
    distinct_paths,
    distinct_hosts,
    distinct_user_agents,
    peak_requests_per_minute,
    client_error_ratio,
    first_seen_at,
    last_seen_at,
    sample_probe_paths,
    sample_hosts,
    sample_user_agents,

    case
        when is_verified_crawler then 0
        else least(100, pts_probe + pts_errors + pts_breadth + pts_burst
                        + pts_datacenter + pts_origin + pts_ua_rotation)
    end                                                                 as threat_score,

    list_filter([
        case when pts_probe > 0 then 'probe_paths' end,
        case when pts_errors > 0 then 'mostly_errors' end,
        case when pts_breadth > 0 then 'wide_scan' end,
        case when pts_burst > 0 then 'burst' end,
        case when pts_datacenter > 0 then 'datacenter' end,
        case when pts_origin > 0 then 'origin_direct' end,
        case when pts_ua_rotation > 0 then 'ua_rotation' end,
        case when is_verified_crawler then 'verified_crawler' end
    ], x -> x is not null)                                              as reasons

from rules
