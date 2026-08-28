-- The privacy guarantee, enforced in the warehouse rather than trusted.
--
-- Python drops the address at the boundary, and the pytest suite asserts that. This
-- is the second line: if a future column, a schema change or a hand-loaded partition
-- ever reintroduces a bare address into the fact table, the build fails here.
--
-- `client_network` is excluded because it is a truncated network by design, and
-- `request_host` because the Host header legitimately contains addresses -- scanners
-- send them, and that is a fact worth keeping rather than laundering.
--
-- Version strings are the false positive to defeat: "Chrome/151.0.0.0" and
-- "WhatsApp/10.0.2.1" are address-shaped and are not addresses. The first version of
-- this test carried a hard-coded list of product names and duly failed on real
-- traffic the first time a product outside the list appeared. The rule that actually
-- holds is structural: an address-shaped token immediately preceded by "/" is a
-- version. RE2, which DuckDB uses, has no lookbehind, so the preceding character is
-- matched explicitly instead -- it must not be a slash, a digit or a dot, which also
-- prevents matching the tail of a longer version string.

{% set address = '(^|[^/0-9.])(\d{1,3}\.){3}\d{1,3}(\D|$)' %}

select
    'path' as column_name, path as offending_value, count(*) as occurrences
from {{ ref('fct_http_request') }}
where regexp_matches(path, '{{ address }}')
group by 1, 2

union all

select
    'user_agent', user_agent, count(*)
from {{ ref('fct_http_request') }}
where regexp_matches(user_agent, '{{ address }}')
group by 1, 2

union all

-- A hash that is not 128 bits of hex did not come from the privacy boundary.
select
    'client_hash', client_hash, count(*)
from {{ ref('fct_http_request') }}
where client_hash is not null
  and not regexp_matches(client_hash, '^[0-9a-f]{32}$')
group by 1, 2
