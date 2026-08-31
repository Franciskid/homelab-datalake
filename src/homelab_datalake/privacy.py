"""The PII boundary. A client address goes in; an address never comes out.

This is the only place a raw client address is allowed to exist, and it exists
only in memory. Everything written to disk or object storage has been through
`redact_client`, which returns a salted hash, a coarse network, and a trust
label -- and drops the original.

The ordering matters and is the whole design: geo and ASN are derived *while*
the address is still in hand (see homelab_datalake.geo), and only then is it
discarded. That keeps every chart the address would have enabled -- country,
city, network operator, bot-versus-human -- without keeping anything that
identifies a household. There is deliberately no "raw landing zone we clean up
later", because that zone is the leak.
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

    Query strings carry password-reset tokens, session ids and API keys often enough
    that storing them is a bet with no upside. The path is what analysis needs.
    """
    if uri is None:
        return None
    return uri.split("?", 1)[0]


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
