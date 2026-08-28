"""Token verification: that a signature is actually checked, not just parsed.

The token arrives from the caller. Decoding it and reading the claims -- which is
correct where a token came straight from Keycloak over TLS -- would here mean
trusting JSON that the caller wrote. So the interesting test is not the happy path,
it is the forged one.
"""

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from homelab_datalake.mcp.auth import REQUIRED_ROLE, KeycloakTokenVerifier

ISSUER = "https://sso.example.org/realms/homelab"
CLIENT = "datalake-mcp"


def _key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _token(private_key, *, roles=(REQUIRED_ROLE,), issuer=ISSUER, client=CLIENT, expires_in=300):
    claims = {
        "iss": issuer,
        "azp": client,
        "sub": "service-account-datalake-mcp",
        "exp": int(time.time()) + expires_in,
        "iat": int(time.time()),
        "resource_access": {client: {"roles": list(roles)}},
    }
    return jwt.encode(claims, private_key, algorithm="RS256")


@pytest.fixture
def verifier(monkeypatch):
    """A verifier whose key set is a single known key."""
    signing_key = _key()
    checker = KeycloakTokenVerifier(issuer=ISSUER, client_id=CLIENT)

    class _Key:
        key = signing_key.public_key()

    monkeypatch.setattr(checker._keys, "get_signing_key_from_jwt", lambda _token: _Key())
    return checker, signing_key


async def _verify(verifier, token):
    return await verifier.verify_token(token)


def test_a_properly_signed_token_with_the_role_is_accepted(verifier):
    import asyncio

    checker, key = verifier
    result = asyncio.run(_verify(checker, _token(key)))
    assert result is not None
    assert REQUIRED_ROLE in result.scopes


def test_a_forged_signature_is_rejected(verifier):
    """Signed with a key the realm never published -- the case that matters."""
    import asyncio

    checker, _ = verifier
    forged = _token(_key())  # a different keypair entirely
    assert asyncio.run(_verify(checker, forged)) is None


def test_a_token_without_the_role_is_rejected(verifier):
    import asyncio

    checker, key = verifier
    assert asyncio.run(_verify(checker, _token(key, roles=("some:other",)))) is None


def test_an_expired_token_is_rejected(verifier):
    import asyncio

    checker, key = verifier
    assert asyncio.run(_verify(checker, _token(key, expires_in=-600))) is None


def test_a_token_from_another_issuer_is_rejected(verifier):
    import asyncio

    checker, key = verifier
    other = _token(key, issuer="https://sso.example.org/realms/other")
    assert asyncio.run(_verify(checker, other)) is None


def test_a_role_granted_on_a_different_client_does_not_count(verifier):
    """Authorisation is a client role on *this* client, not a role of the same name."""
    import asyncio

    checker, key = verifier
    elsewhere = _token(key, client="some-other-client")
    assert asyncio.run(_verify(checker, elsewhere)) is None


def test_serving_over_the_network_without_an_issuer_refuses_to_start(monkeypatch):
    """Down is a better outcome than open: an unauthenticated endpoint looks fine."""
    from homelab_datalake.mcp import server as server_module

    monkeypatch.setattr(server_module, "_verifier", None)
    monkeypatch.setenv("MCP_TRANSPORT", "streamable-http")
    with pytest.raises(SystemExit, match="MCP_OIDC_ISSUER"):
        server_module.main()
