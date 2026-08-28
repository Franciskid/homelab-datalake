"""PII boundary tests.

The property under test is not "the hash is correct" but "the address does not
survive". A bug here is a data leak, not a wrong number, so these tests assert
absence as much as presence.

Every address below is from a documentation range (RFC 5737 / RFC 3849).
"""

import ipaddress
from dataclasses import asdict

import pytest

from homelab_datalake.privacy import (
    CLOUDFLARE_RANGES,
    TRUST_CDN_EDGE,
    TRUST_RESOLVED,
    TRUST_UNKNOWN,
    hash_client,
    network_of,
    classify_trust,
    redact_client,
    strip_query,
)

SALT = "test-salt-not-the-production-one"


def test_redacted_client_never_carries_the_address():
    """The strongest guarantee this module makes, asserted structurally."""
    address = "203.0.113.42"
    result = redact_client(address, SALT)
    assert "raw" not in asdict(result)
    assert address not in repr(result)
    assert all(address not in str(v) for v in asdict(result).values())


def test_hash_is_stable_for_the_same_address_and_salt():
    """Returning-visitor counts depend on this, so it is a contract, not a detail."""
    assert hash_client("203.0.113.42", SALT) == hash_client("203.0.113.42", SALT)


def test_hash_differs_between_addresses_and_between_salts():
    assert hash_client("203.0.113.42", SALT) != hash_client("203.0.113.43", SALT)
    assert hash_client("203.0.113.42", SALT) != hash_client("203.0.113.42", "other-salt")


def test_hash_is_128_bits_of_hex():
    assert len(hash_client("203.0.113.42", SALT)) == 32


def test_empty_salt_is_refused_rather_than_producing_a_weak_hash():
    """An unsalted digest over 2^32 addresses is reversible in minutes."""
    with pytest.raises(ValueError, match="CLIENT_IP_HASH_SALT"):
        hash_client("203.0.113.42", "")


def test_ipv4_is_truncated_to_a_24():
    assert network_of("203.0.113.42") == "203.0.113.0/24"


def test_ipv6_is_truncated_to_a_48():
    assert network_of("2001:db8:1234:5678::1") == "2001:db8:1234::/48"


def test_network_of_rejects_nonsense():
    assert network_of("not-an-address") is None


@pytest.mark.parametrize("address", ["172.71.130.17", "104.23.166.63", "141.101.68.93"])
def test_cloudflare_addresses_are_labelled_as_edges(address):
    assert classify_trust(address) == TRUST_CDN_EDGE


@pytest.mark.parametrize("address", ["203.0.113.200", "2001:db8:dead:beef::1", "203.0.113.1"])
def test_visitor_addresses_are_labelled_resolved(address):
    assert classify_trust(address) == TRUST_RESOLVED


def test_docker_range_is_not_mistaken_for_cloudflare():
    """172.16.0.0/12 (Docker) and 172.64.0.0/13 (Cloudflare) must not be confused."""
    assert classify_trust("172.18.0.1") == TRUST_RESOLVED
    assert classify_trust("172.64.0.1") == TRUST_CDN_EDGE


@pytest.mark.parametrize("value", [None, "", "not-an-address"])
def test_unusable_values_are_unknown_not_resolved(value):
    """Absent is a different fact from present-and-direct, and must not round to it."""
    assert classify_trust(value) == TRUST_UNKNOWN


def test_malformed_address_is_still_not_stored():
    result = redact_client("999.999.999.999", SALT)
    assert result.client_hash is None
    assert result.client_trust == TRUST_UNKNOWN


def test_redaction_reports_address_version():
    assert redact_client("203.0.113.42", SALT).client_version == 4
    assert redact_client("2001:db8::1", SALT).client_version == 6


def test_is_private_separates_lan_traffic_from_the_internet():
    assert redact_client("10.0.0.10", SALT).is_private is True
    assert redact_client("10.0.0.5", SALT).is_private is True
    assert redact_client("8.8.8.8", SALT).is_private is False


def test_documentation_ranges_count_as_private():
    """A gotcha worth pinning: RFC 5737 / RFC 3849 ranges are not globally
    reachable, so `is_private` is True for them. Most fixtures in this suite use
    those ranges, and a test that assumed otherwise would pass for the wrong
    reason. Real visitor traffic is unaffected -- no one browses from 203.0.113/24."""
    assert redact_client("203.0.113.42", SALT).is_private is True
    assert redact_client("2001:db8::1", SALT).is_private is True


def test_cloudflare_range_list_is_well_formed():
    """A typo here would silently mislabel a whole provider."""
    for cidr in CLOUDFLARE_RANGES:
        ipaddress.ip_network(cidr)  # raises on a malformed entry
    assert len(CLOUDFLARE_RANGES) == 22


def test_query_strings_are_dropped_and_paths_kept():
    assert strip_query("/reset?token=secret123") == "/reset"
    assert strip_query("/api/v3/queue") == "/api/v3/queue"
    assert strip_query("/a?b=1?c=2") == "/a"
    assert strip_query(None) is None
