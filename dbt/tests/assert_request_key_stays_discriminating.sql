-- No two requests share a `request_key` while differing in anything the table records.
--
-- That is what "discriminating" means for this key, stated directly. The key is not
-- unique and must not be: two requests identical in every recorded respect -- same
-- second, client, host, method, path, query, status, bytes and user agent -- are
-- indistinguishable in the log, and inventing a difference would be inventing data.
-- What must never happen is the other case, a key that merges requests the table can
-- tell apart. Every distinct-count built on the key then undercounts, silently.
--
-- **It compares every column, read from the warehouse.** `adapter.get_columns_in_relation`
-- asks what fct_http_request actually holds, so a column added to the fact and left out
-- of the key fails this test the first night it varies. That is not hypothetical: it is
-- how this key broke. query_string reached the fact on 14 Sep 2026 and not the key, and
-- by the 19th the key was merging 718k rows of Keycloak's OIDC auth endpoint whose query
-- strings differed in `state` and `nonce` -- plus 2,923 groups differing in user agent,
-- 527 in method, 73 in host. The key now covers every recorded field, and on the day it
-- changed this found zero offending keys across all 11,063,927 rows.
--
-- The test it replaces measured each day's duplicate share against a 40% threshold, as
-- a proxy for the key's health. The proxy could not tell a key failing from traffic
-- repeating, and by 19 Sep it was warning on ten days at up to 95% -- every one of them
-- genuine repeats: the camera's HLS player caught in a login redirect loop, requesting
-- one playlist 30-55 times a second for hours (2.53M times on 13 Sep from one browser),
-- each request bounced to /api/auth/login and on to Keycloak and retried. Those rows
-- are identical in every column, so they pass here, correctly. A loop like that is a
-- traffic problem for monitoring to catch, not a data-quality one: nothing about the
-- table was wrong.
--
-- It fails the build, where the threshold only warned. A threshold on traffic has to
-- tolerate a noisy night; this has no noisy nights -- a merged pair of distinguishable
-- requests is wrong on a quiet day and on a flood day alike, and every number built on
-- the key is wrong with it.
--
-- The weaker failure -- an input still present but no longer saying anything, like a
-- timestamp that stops parsing -- makes rows identical rather than different, so it
-- cannot surface here. assert_request_key_inputs_stay_populated watches for it.
--
-- Two stages, because the second is expensive and almost always runs on nothing: the
-- first finds offending keys with one hash per row, the second names the columns that
-- differ, for those keys only.

{%- set columns = [] -%}
{%- if execute -%}
    {%- for column in adapter.get_columns_in_relation(ref('fct_http_request')) -%}
        {%- if column.name != 'request_key' -%}
            {%- do columns.append(column.name) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if not columns -%}
        {{ exceptions.raise_compiler_error(
            "assert_request_key_stays_discriminating found no columns in fct_http_request. "
            "A comparison over nothing passes for everything, so this refuses to run.") }}
    {%- endif -%}
{%- endif %}

with offending as (

    select request_key
    from {{ ref('fct_http_request') }}
    group by 1
    having count(distinct hash(
        {%- for column in columns %}
        {{ adapter.quote(column) }}{{ "," if not loop.last }}
        {%- endfor %}
    )) > 1

)

select
    request_key,
    count(*)                                    as rows_sharing_key,
    min(partition_date)                         as first_day,
    max(partition_date)                         as last_day,
    list_filter([
        {%- for column in columns %}
        case when count(distinct coalesce(cast({{ adapter.quote(column) }} as varchar), '<null>')) > 1
             then '{{ column }}' end{{ "," if not loop.last }}
        {%- endfor %}
    ], c -> c is not null)                      as differing_columns
from {{ ref('fct_http_request') }}
where request_key in (select request_key from offending)
group by 1
