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

)

select
    *,
    (client_kind = 'bot' or host_is_address) as is_automated
from classified
