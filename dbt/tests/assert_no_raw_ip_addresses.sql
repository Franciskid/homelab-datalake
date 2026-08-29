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
-- Version strings are the false positive to defeat, and this rule has now been wrong
-- twice, in the same way both times.
--
-- First it carried a hard-coded list of product names, and failed on real traffic the
-- moment a product outside the list appeared ("WhatsApp/10.0.2.1"). That was replaced
-- with a structural rule: a token immediately preceded by "/" is a version. Which
-- then failed on
--
--     Mozilla/5.0 (X11; Linux x86_64; rv:1.9.5.20) Gecko/ Firefox/9.0
--
-- because Gecko introduces its version with "rv:" and not with a slash. Both times
-- the vocabulary came from the sample in front of me rather than from the format.
--
-- So: "/" or ":" immediately before the token means a version. A leaked address is
-- not written that way -- a header value that reached the wrong column arrives as
-- "X-Real-IP: 203.0.113.4", with a space, and is still caught. Nothing
-- numeric distinguishes 1.9.5.20 from an address, so position is the only signal there is.
--
-- RE2, which DuckDB uses, has no lookbehind, so the preceding character is matched
-- explicitly -- it must not be a slash, a colon, a digit or a dot, the last two
-- preventing a match on the tail of a longer version string.
--
-- The pytest guard over the repository deliberately does NOT allow ":" here. It
-- defends against a different thing: a human pasting a production log line into a
-- file, where "X-Real-IP:203.0.113.200" is exactly the shape to catch. This test's
-- input is machine-generated client text that is full of legitimate versions.

{% set address = '(^|[^/:0-9.])(\d{1,3}\.){3}\d{1,3}(\D|$)' %}

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
