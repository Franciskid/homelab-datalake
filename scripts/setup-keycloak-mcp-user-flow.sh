#!/usr/bin/env bash
# Configure datalake-mcp for a client that logs a *person* in, rather than a service.
#
# Two different flows need two different setups, and mixing them up is why an
# endpoint can answer curl perfectly and still refuse every real client:
#
#   client_credentials   a program authenticating as itself. Already configured by
#                        setup-keycloak-mcp.sh. The token carries the service
#                        account's roles.
#
#   authorization_code   a connector logging YOU in through a browser. The token
#                        then carries *your* roles, so the datalake:query role has
#                        to be granted to your user as well -- granting it to the
#                        service account does nothing for this flow. That is the
#                        step most easily missed.
#
# This script adds the second without removing the first.
#
# REDIRECT_URI must be the callback your client actually uses. The default is
# Claude's web connector callback; Claude Desktop and other local clients use a
# http://localhost:PORT/... callback instead, and the client tells you which.

set -euo pipefail

ADMIN_REALM="${ADMIN_REALM:-master}"
ADMIN_USER="${ADMIN_USER:-operator}"
REALM="${REALM:-homelab}"
CONTAINER="${KEYCLOAK_CONTAINER:-keycloak}"
SERVER="${KEYCLOAK_URL:-http://localhost:8282}"
CLIENT_ID="${CLIENT_ID:-datalake-mcp}"
ROLE="${ROLE:-datalake:query}"

# Who should be able to query through a browser login.
GRANT_TO="${GRANT_TO:-operator}"

REDIRECT_URI="${REDIRECT_URI:-https://claude.ai/api/mcp/auth_callback}"

kc() { docker exec -i "$CONTAINER" /opt/keycloak/bin/kcadm.sh "$@"; }

echo "Logging in to $SERVER as $ADMIN_USER in realm $ADMIN_REALM."
docker exec -it "$CONTAINER" /opt/keycloak/bin/kcadm.sh config credentials \
  --server "$SERVER" --realm "$ADMIN_REALM" --user "$ADMIN_USER"

UUID=$(kc get clients -r "$REALM" -q "clientId=$CLIENT_ID" --fields id --format csv --noquotes | tr -d '\r')
if [ -z "$UUID" ]; then
  echo "Client $CLIENT_ID does not exist. Run setup-keycloak-mcp.sh first." >&2
  exit 1
fi

echo "Enabling the browser flow on $CLIENT_ID with redirect $REDIRECT_URI"
kc update "clients/$UUID" -r "$REALM" \
  -s 'standardFlowEnabled=true' \
  -s "redirectUris=[\"$REDIRECT_URI\"]" \
  -s 'webOrigins=["+"]' \
  -s 'attributes."pkce.code.challenge.method"=S256'
echo "  done. PKCE S256 required, so an intercepted code cannot be exchanged."

echo "Granting $ROLE to the user $GRANT_TO"
UID_=$(kc get users -r "$REALM" -q "username=$GRANT_TO" --fields id --format csv --noquotes | tr -d '\r')
if [ -z "$UID_" ]; then
  echo "  no user $GRANT_TO in realm $REALM" >&2; exit 1
fi
kc add-roles -r "$REALM" --uid "$UID_" --cclientid "$CLIENT_ID" --rolename "$ROLE"
echo "  done."

echo
echo "Client id:     $CLIENT_ID"
echo "Client secret:"
kc get "clients/$UUID/client-secret" -r "$REALM" --fields value --format csv --noquotes

cat <<'NEXT'

Give the connector:

  Server URL     https://data.example.test/mcp
  Client ID      datalake-mcp
  Client secret  (above)

If the client insists on registering itself instead of taking a client id, it needs
Dynamic Client Registration, which this realm currently refuses for everything --
Keycloak's "Trusted Hosts" policy ships with an empty host list. Prefer giving the
client id above; opening DCR means anyone on the internet can create clients in the
realm.
NEXT
