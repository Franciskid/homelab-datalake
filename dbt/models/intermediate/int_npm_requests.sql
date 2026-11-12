-- Request rows with the two derived dimensions the marts actually group by.
--
-- Bot classification is deliberately conservative and user-agent based only. The
-- honest alternative -- ASN-based, "datacentre ranges are not people" -- needs the
-- MaxMind database, which is optional (see IMPLEMENTATION_PLAN.md section 14). When
-- geo is available the ASN signal is added; when it is not, this still works and the
-- column says which evidence it used rather than pretending to certainty.

with requests as (

    select * from {{ ref('stg_npm_access') }}

),

classified as (

    select
        *,
        case
            when user_agent is null then 'unknown'
            when regexp_matches(lower(user_agent),
                 '(bot|crawler|spider|scanner|curl|wget|python-requests|go-http-client|libwww|okhttp)')
                then 'bot'
            when regexp_matches(lower(user_agent), '(mozilla|safari|chrome|firefox|edge)')
                then 'browser'
            else 'other'
        end as client_kind,

        -- Path grouped to its first two segments. Full paths are unbounded cardinality
        -- (every movie id is its own path); the first two segments are what a person
        -- actually wants to see grouped in a dashboard.
        case
            when path is null or path = '' then '/'
            else '/' || array_to_string(
                     list_filter(string_split(path, '/'), x -> x <> '')[1:2], '/')
        end as path_group

    from requests

),

origin_network as (
    select
        r.*,
        o.origin_edge,
        o.evidence as managed_origin_evidence,
        case
            when r.client_trust is distinct from 'resolved' then 'unresolved'
            when o.address is not null then 'managed_network'
            when r.is_internal then 'private_network'
            when r.client_address is null then 'unresolved'
            else 'unmanaged_network'
        end as origin_network_scope,
        case
            when starts_with(lower(coalesce(r.user_agent, '')), 'blackbox-exporter/')
                then 'monitoring'
            when r.path like '/datalake/inbox/npm/%'
                 and starts_with(lower(coalesce(r.user_agent, '')), 'minio (')
                 and contains(lower(r.user_agent), ' mc/')
                then 'edge_log_mirror'
            when (r.path = '/datalake' or r.path like '/datalake/%'
                  or r.path = '/datalake-backup' or r.path like '/datalake-backup/%')
                 and starts_with(lower(coalesce(r.user_agent, '')), 'duckdb/')
                then 'datalake_query'
            when (r.path = '/datalake' or r.path like '/datalake/%'
                  or r.path = '/datalake-backup' or r.path like '/datalake-backup/%')
                 and (starts_with(lower(coalesce(r.user_agent, '')), 'boto3/')
                      or starts_with(lower(coalesce(r.user_agent, '')), 'aws-sdk-js/')
                      or (starts_with(lower(coalesce(r.user_agent, '')), 'minio (')
                          and contains(lower(r.user_agent), ' mc/')))
                then 'datalake_storage'
        end as service_signature
    from classified r
    left join {{ ref('int_managed_origin_addresses') }} o
        on r.client_trust = 'resolved'
       and r.client_address = o.address
       and r.requested_at >= o.valid_from
       and (o.valid_until is null or r.requested_at < o.valid_until)
)

select
    * exclude (managed_origin_evidence, origin_network_scope, service_signature),
    (client_kind = 'bot' or host_is_address) as is_automated,
    case
        when origin_network_scope in ('managed_network', 'private_network')
             and service_signature is not null then true
        when origin_network_scope = 'unmanaged_network' then false
        else null
    end as is_internal_service,
    case
        when origin_network_scope in ('managed_network', 'private_network')
             and service_signature is not null then 'internal_service'
        else origin_network_scope
    end as traffic_origin,
    case when origin_network_scope in ('managed_network', 'private_network')
         then service_signature end as origin_service,
    case
        when origin_network_scope in ('managed_network', 'private_network')
             and service_signature is not null then 'inferred'
        when origin_network_scope = 'unresolved' then 'unknown'
        else 'network_only'
    end as origin_confidence,
    case
        when origin_network_scope = 'unresolved' then 'No resolved client address; service origin cannot be attributed'
        when origin_network_scope = 'unmanaged_network' then 'No managed address binding at request time; user agent alone is insufficient'
        else coalesce('Managed edge ' || origin_edge || ': ' || managed_origin_evidence,
                      'Private client address; source edge is not identified')
             || case when service_signature is not null
                     then '; service signature: ' || service_signature || ' (inferred from user agent and request path)'
                     else '; no service signature; a browser may share this egress' end
    end as origin_evidence
from origin_network
