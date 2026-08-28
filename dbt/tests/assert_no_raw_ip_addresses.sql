-- The privacy guarantee, enforced in the warehouse rather than trusted.
--
-- Python drops the address at the boundary, and the pytest suite asserts that. This
-- is the second line: if a future column, a schema change or a hand-loaded partition
-- ever reintroduces a bare address into the fact table, the build fails here.
--
-- `client_network` is excluded because it is a truncated network by design, and
-- `request_host` because the Host header legitimately contains addresses -- scanners
-- send them, and that is a fact worth keeping rather than laundering.

select
    'path' as column_name, path as offending_value, count(*) as occurrences
from {{ ref('fct_http_request') }}
where regexp_matches(path, '\b(\d{1,3}\.){3}\d{1,3}\b')
group by 1, 2

union all

select
    'user_agent', user_agent, count(*)
from {{ ref('fct_http_request') }}
where regexp_matches(user_agent, '\b(\d{1,3}\.){3}\d{1,3}\b')
  -- Version strings look like addresses. "Chrome/151.0.0.0" is not a leak.
  and not regexp_matches(user_agent, '(?i)(chrome|firefox|safari|version|edge|opr|msie)/[\d.]+')
group by 1, 2

union all

select
    'client_hash', client_hash, count(*)
from {{ ref('fct_http_request') }}
where client_hash is not null
  and not regexp_matches(client_hash, '^[0-9a-f]{32}$')
group by 1, 2
