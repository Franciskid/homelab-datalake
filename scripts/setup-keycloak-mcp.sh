#!/usr/bin/env bash
# Create the Keycloak client the MCP endpoint authenticates against.
#
# Run this on the host where Keycloak runs. It needs realm-admin credentials, so it
# asks for them interactively rather than reading them from anywhere -- and nothing
# it prints includes a password.
#
# What it creates, and why each part:
#
#   datalake-mcp          a confidential client with a service account. Service
#                         account, not a user: the caller is a program, and a
#                         browser login is not something it can complete.
#   datalake:query        a CLIENT role on that client, assigned to its own service
#                         account. A client role rather than a realm one keeps it
#                         scoped -- a realm role of the same name could be granted to
#                         something unrelated by accident, and this cannot.
#
# Standard flow and direct grants are both off: client_credentials is the only way to
# get a token, so there is no login page and no password anywhere in the picture.
#
# The client secret is printed once, at the end. Put it wherever the caller keeps its
# credentials. The datalake itself does not need it -- it verifies tokens, it does not
# mint them.

set -euo pipefail

REALM="${REALM:-homelab}"
CONTAINER="${KEYCLOAK_CONTAINER:-keycloak}"
SERVER="${KEYCLOAK_URL:-http://localhost:8282}"
CLIENT_ID="${CLIENT_ID:-datalake-mcp}"
ROLE="${ROLE:-datalake:query}"

kc() { docker exec -i "$CONTAINER" /opt/keycloak/bin/kcadm.sh "$@"; }

ADMIN_USER="${ADMIN_USER:-admin}"

echo "Logging in to $SERVER as $ADMIN_USER (override with ADMIN_USER=...)."
# No --password: kcadm prompts for it itself, so the password is never an argument,
# never in this script, and never in the shell history.
docker exec -it "$CONTAINER" /opt/keycloak/bin/kcadm.sh config credentials \
  --server "$SERVER" --realm master --user "$ADMIN_USER"

echo
echo "Creating client $CLIENT_ID in realm $REALM..."
if kc get "clients?clientId=$CLIENT_ID" -r "$REALM" --fields id --format csv --noquotes | grep -q .; then
  echo "  already exists -- leaving it alone."
else
  kc create clients -r "$REALM" \
    -s "clientId=$CLIENT_ID" \
    -s 'name=Datalake MCP' \
    -s 'description=Typed analytical tools over the homelab warehouse.' \
    -s 'protocol=openid-connect' \
    -s 'publicClient=false' \
    -s 'serviceAccountsEnabled=true' \
    -s 'standardFlowEnabled=false' \
    -s 'directAccessGrantsEnabled=false' \
    -s 'enabled=true'
  echo "  created."
fi

CLIENT_UUID=$(kc get "clients?clientId=$CLIENT_ID" -r "$REALM" --fields id --format csv --noquotes | tr -d '\r')

echo "Creating client role $ROLE..."
kc create "clients/$CLIENT_UUID/roles" -r "$REALM" -s "name=$ROLE" \
  -s "description=May query the warehouse through the MCP tools." 2>/dev/null \
  && echo "  created." || echo "  already exists."

SA_USER=$(kc get "clients/$CLIENT_UUID/service-account-user" -r "$REALM" \
  --fields id --format csv --noquotes | tr -d '\r')

echo "Assigning $ROLE to the service account..."
kc add-roles -r "$REALM" --uid "$SA_USER" --cclientid "$CLIENT_ID" --rolename "$ROLE"
echo "  done."

echo
echo "Client secret for $CLIENT_ID (store it with the caller, not in this repo):"
kc get "clients/$CLIENT_UUID/client-secret" -r "$REALM" --fields value --format csv --noquotes

cat <<'NEXT'

Check it end to end:

  TOKEN=$(curl -s -X POST \
    https://auth.example.test/realms/homelab/protocol/openid-connect/token \
    -d grant_type=client_credentials \
    -d client_id=datalake-mcp \
    -d client_secret=<the secret above> | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')

  curl -s https://data.example.test/mcp -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | head -c 400

Expect a list of tools. Without the header the same request returns 401, which is
what it does today.
NEXT
