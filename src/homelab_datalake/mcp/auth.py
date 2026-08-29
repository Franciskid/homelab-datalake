"""Bearer-token authentication for the MCP server, against the same Keycloak realm.

The camera export already works this way, and doing it twice the same way is the
point: every account and every grant lives in one realm, so revoking something is a
toggle in Keycloak rather than an edit to a file on a server nobody remembers.

The token arrives from whoever made the request, so its signature is the only thing
separating a real caller from someone who wrote their own JSON. It is verified
against the realm's published keys on every request -- not decoded and trusted.

Two failure modes are deliberately closed:

* **No issuer configured, HTTP transport requested** -- the server refuses to start
  rather than listening without authentication. An analytics endpoint that answers
  anyone is a worse outcome than an endpoint that is down, because nothing about it
  looks wrong.
* **A token for another client** -- authorisation is a *client* role on this client,
  read from `resource_access[client_id].roles`. A realm role could be granted to
  something unrelated by accident; this cannot.
"""

from __future__ import annotations

import logging
import os

import jwt
from mcp.server.auth.provider import AccessToken

from homelab_datalake.useragent import DEFAULT_HEADERS

logger = logging.getLogger(__name__)

REQUIRED_ROLE = "datalake:query"

# Keycloak issues client_credentials tokens with an audience of "account", so the
# audience is not what authorises here -- the role below is, and it is bound to this
# client's own entry, which a token minted for a different client cannot satisfy.
_VERIFY_AUDIENCE = False


class KeycloakTokenVerifier:
    """Verifies a Keycloak access token and requires the datalake:query role."""

    def __init__(self, issuer: str, client_id: str) -> None:
        self.issuer = issuer.rstrip("/")
        self.client_id = client_id
        # PyJWKClient caches the key set and refetches on an unknown `kid`, so a key
        # rotation in Keycloak is picked up without a restart and without a fetch per
        # request.
        #
        # The User-Agent is load-bearing, not cosmetic. Keycloak sits behind
        # Cloudflare, which answers urllib's default agent with a 403 before the
        # request reaches the origin -- and PyJWKClient uses urllib. The result was
        # that every properly signed token was rejected, because the signing keys
        # could not be downloaded at all. It presents as an auth failure and is a
        # network one, which is the third time that trap has been sprung here.
        self._keys = jwt.PyJWKClient(
            f"{self.issuer}/protocol/openid-connect/certs",
            cache_keys=True,
            lifespan=600,
            headers=DEFAULT_HEADERS,
        )

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            key = self._keys.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256", "ES256"],
                issuer=self.issuer,
                options={"verify_aud": _VERIFY_AUDIENCE},
                leeway=30,
            )
        except jwt.PyJWKClientError as error:
            # Not a bad token: the key set could not be fetched or did not contain the
            # key. Logged loudly because it rejects *every* caller and is an outage,
            # not a scan -- and it is indistinguishable from an auth failure to whoever
            # is on the other end.
            logger.error(
                "cannot verify tokens: the JWKS at %s could not be read (%s). "
                "Every request will be refused until this is fixed.",
                f"{self.issuer}/protocol/openid-connect/certs", error,
            )
            return None
        except jwt.PyJWTError as error:
            # Logged at debug: a failed verification is an ordinary event on a public
            # endpoint, and logging it loudly would turn a scan into an alert storm.
            logger.debug("token rejected: %s", error)
            return None

        roles = (claims.get("resource_access", {}).get(self.client_id, {})).get("roles", [])
        if REQUIRED_ROLE not in roles:
            logger.debug("token lacks %s (has %s)", REQUIRED_ROLE, roles)
            return None

        return AccessToken(
            token=token,
            client_id=claims.get("azp") or self.client_id,
            scopes=list(roles),
            expires_at=claims.get("exp"),
        )


def verifier_from_env() -> KeycloakTokenVerifier | None:
    issuer = os.environ.get("MCP_OIDC_ISSUER", "").strip()
    if not issuer:
        return None
    return KeycloakTokenVerifier(
        issuer=issuer,
        client_id=os.environ.get("MCP_OIDC_CLIENT_ID", "datalake-mcp"),
    )
