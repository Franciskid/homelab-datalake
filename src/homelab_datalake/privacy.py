"""The PII boundary. A client address goes in; `redact_client` never lets it out.

For a year this docstring said an address never comes out at all. That changed on
2026-09-14: the owner decided the warehouse should hold the raw client address and
the raw query string too, alongside the derived columns below, because a lakehouse
that hashes away the thing someone might one day need to look up is not holding "all
available data" -- it is holding a summary of it. The derived columns did not stop
existing or stop being the default: `redact_client` is unchanged, every existing
chart -- country, city, network operator, bot-versus-human -- still runs off the hash
and the network, and nothing here got easier to de-anonymise by looking at one row.

What changed is that `homelab_datalake.parsing.extract` now *also* writes
`parsed.client` and `raw_query_string(parsed.uri)` straight into `raw.npm_access`
(and the mcp-api equivalent), unredacted, alongside `client_hash`/`client_network`.
Two things keep that from being the leak this module used to promise could not
happen:

* **It is scoped to `raw/`, `curated/` and `marts/` in the private warehouse.**
  Those layers are reachable only through Keycloak-gated tools (the MCP server, the
  workbench API, dbt/notebook access on the deployed host) -- never through anything
  that ships in this git repository. `seed/` is gitignored and synthetic either way
  (see `scripts/generate_seed.py`), and `homelab_datalake.assets.publish` /
  `homelab_datalake.serving` never touch a wider audience than they did before this
  change; see the tests exercising those modules for the assertion, not just this
  comment.
* **Geo and network derivation still happen first, and `redact_client` still runs
  unconditionally.** The raw columns are additive, not a replacement -- a caller who
  only ever reads `client_hash`/`client_network`/`client_trust` sees the exact same
  pipeline as before. The raw columns exist for the caller who has a legitimate
  reason to look a request up by the address that made it, on the deployed instance,
  which nobody outside the household can reach.

NPM keeps roughly five weeks of lines before logrotate recycles them (see
`LOG_RETENTION_START` in `assets/npm_access.py`), so this cannot be backfilled past
what is still on disk the day it ships: older partitions keep null raw columns
forever, and `mcp.semantic.WEB_TRAFFIC`'s description says from which date onward
they are populated.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
from dataclasses import dataclass

# Cloudflare's published ranges, from https://www.cloudflare.com/ips-v4 and ips-v6.
# Used only to *label* an address as a CDN edge rather than a visitor. It is not a
# trust decision -- nginx makes that, with its own list (see infra/npm/README.md).
# Refresh with scripts/check-cloudflare-ranges.sh, which reports drift.
CLOUDFLARE_RANGES: tuple[str, ...] = (
    "173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
    "141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20",
    "197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13",
    "104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22",
    "2400:cb00::/32", "2606:4700::/32", "2803:f800::/32", "2405:b500::/32",
    "2405:8100::/32", "2a06:98c0::/29", "2c0f:f248::/32",
)

_CLOUDFLARE_NETS = tuple(ipaddress.ip_network(cidr) for cidr in CLOUDFLARE_RANGES)

# How much of an address survives as a coarse network. /24 and /48 are the usual
# GDPR-pseudonymisation choices: wide enough not to single out a household, narrow
# enough to still separate a datacentre range from a home ISP.
_IPV4_PREFIX = 24
_IPV6_PREFIX = 48

TRUST_CDN_EDGE = "cdn_edge"
TRUST_RESOLVED = "resolved"
TRUST_UNKNOWN = "unknown"


@dataclass(frozen=True)
class RedactedClient:
    """What survives of a client address. Deliberately has no raw field."""

    client_hash: str | None
    client_network: str | None
    client_trust: str
    client_version: int | None
    is_private: bool


def classify_trust(address: str | None) -> str:
    """Does this value name a visitor, or a CDN machine standing in for one?

    Two labels, not three. Before the 27 Aug 2026 cutover (see IMPLEMENTATION_PLAN
    section 6) a Cloudflare-proxied request logged the edge; after it, the visitor.
    But a restored address and an always-direct one are byte-identical, so there is
    no honest way to tell them apart from the row alone. Inventing a third label
    would be asserting something the data does not contain.
    """
    if not address:
        return TRUST_UNKNOWN
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return TRUST_UNKNOWN
    return TRUST_CDN_EDGE if any(ip in net for net in _CLOUDFLARE_NETS) else TRUST_RESOLVED


def hash_client(address: str, salt: str) -> str:
    """Salted HMAC-SHA256, truncated to 16 bytes.

    HMAC rather than a plain salted digest because the salt is a key here, not a
    per-record nonce: an attacker with the address space (there are only 2^32 IPv4
    addresses) can brute-force any unkeyed digest in minutes. Truncation to 128 bits
    keeps collisions negligible at this cardinality while halving the column width.
    """
    if not salt:
        raise ValueError("CLIENT_IP_HASH_SALT is required; refusing to write a weak hash")
    digest = hmac.new(salt.encode("utf-8"), address.encode("utf-8"), hashlib.sha256).digest()
    return digest[:16].hex()


def network_of(address: str) -> str | None:
    """The address truncated to its /24 (IPv4) or /48 (IPv6)."""
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return None
    prefix = _IPV4_PREFIX if ip.version == 4 else _IPV6_PREFIX
    return str(ipaddress.ip_network(f"{ip}/{prefix}", strict=False))


def redact_client(address: str | None, salt: str) -> RedactedClient:
    """Turn a client address into what is safe to store. The input is not returned."""
    if not address:
        return RedactedClient(None, None, TRUST_UNKNOWN, None, False)
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        # A malformed address is still not stored. It cannot be hashed usefully and
        # keeping it raw would be exactly the leak this module exists to prevent.
        return RedactedClient(None, None, TRUST_UNKNOWN, None, False)
    return RedactedClient(
        client_hash=hash_client(address, salt),
        client_network=network_of(address),
        client_trust=classify_trust(address),
        client_version=ip.version,
        is_private=ip.is_private,
    )


def strip_query(uri: str | None) -> str | None:
    """Keep the path, drop the query string.

    Query strings can carry sensitive values often enough that a value-free `path`
    is worth keeping as its own column regardless. Since 2026-09-14 the raw string
    also survives separately, in `query_string` (see `raw_query_string` below) --
    that was a decision to stop discarding the value, not to stop offering the
    value-free path.
    """
    if uri is None:
        return None
    return uri.split("?", 1)[0]


def raw_query_string(uri: str | None) -> str | None:
    """The query string exactly as sent, without the leading `?`.

    Added 2026-09-14, when the owner decided this warehouse should keep the raw
    query value alongside the value-free `path` and the parameter names below, so a
    request can be reconstructed in full when there is a legitimate reason to.

    This value lands only in `raw.npm_access` and its mcp-api counterpart, and in
    the curated/marts models built from them -- all private-warehouse-only, reached
    solely through the Keycloak-gated MCP server and workbench API on the deployed
    host, never through anything this git repository ships. No export, seed or
    publish path may select it; that is enforced in code, not only documented --
    see `tests/test_no_real_addresses_in_fixtures.py` and the export-path tests in
    `tests/test_publish.py` / `tests/test_serving.py`.

    Distinct from an absent query string: returns `None` only when there was no `?`
    at all, and `""` when there was one with nothing after it -- `has_query` is
    true in both of the cases this returns, so this is what tells them apart.
    """
    if uri is None or "?" not in uri:
        return None
    return uri.split("?", 1)[1]


def query_param_names(uri: str | None) -> list[str]:
    """The parameter *names* a request carried, never their values.

    The values are the dangerous half -- a reset token, a session id, an API key --
    and dropping them is right. The names are not: "which endpoints take a `token`
    parameter" and "is anything still calling this with `?debug=`" are ordinary
    questions about one's own estate, and the whole query string being discarded left
    them unanswerable. Sorted and de-duplicated, so the column is a set rather than a
    record of the order somebody happened to write them in.
    """
    if not uri or "?" not in uri:
        return []
    query = uri.split("?", 1)[1]
    names = set()
    for pair in query.split("&"):
        if not pair:
            continue
        name = pair.split("=", 1)[0].strip()
        if name:
            # Bounded: a scanner can put a kilobyte of junk in a parameter name, and
            # this column exists to be grouped by, not to store an attack verbatim.
            names.add(name[:64])
    return sorted(names)
