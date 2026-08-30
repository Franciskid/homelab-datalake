#!/usr/bin/env bash
# Create the Keycloak client the ai-platform gateway uses to reach the datalake API.
#
# Run this on the host where Keycloak runs. It asks for the administrator password
# interactively, so nothing it prints and nothing in your shell history contains one.
#
#   bash scripts/setup-keycloak-datalake-api.sh
#
# WHAT IT CREATES
#
#   ai-platform-datalake   a confidential client with a service account and no login
#                          flow, exactly like datalake-mcp. Its own client, not a
#                          reuse of datalake-mcp's, for two reasons: the two can then
#                          be revoked separately, and the datalake's audit log records
#                          `client_id`, so a workbench query and an MCP tool call stay
#                          distinguishable afterwards.
#
# THE ONE THING THAT IS NOT OBVIOUS
#
# The role it is granted belongs to **datalake-mcp**, not to the client being created.
#
# The datalake verifies a token by reading `resource_access[<client>][roles]`, where
# <client> is MCP_OIDC_CLIENT_ID -- `datalake-mcp` on this deployment. Keycloak keys
# `resource_access` by the client that *defines* a role, not by the one that asked for
# the token. So a `datalake:query` role defined on `ai-platform-datalake` would land
# in the token as `resource_access["ai-platform-datalake"]`, which the datalake does
# not look at, and every request would be refused with a message about a missing role
# that is plainly there in the admin console.
#
# Granting datalake-mcp's own role to this client's service account puts it in
# `resource_access["datalake-mcp"]`, which is where the check reads.
#
# That is also why one role is shared rather than duplicated: access is granted and
# revoked in one place, and removing `datalake:query` from an account removes it
# everywhere it was ever meaningful.

set -euo pipefail

# Two realms doing two different jobs. Mixing them up produces "invalid user
# credentials", which looks like a wrong password and is not.
#
#   ADMIN_REALM  where the administrator account lives, and where the login happens.
#   REALM        where the clients live, alongside every other homelab client.
ADMIN_REALM="${ADMIN_REALM:-master}"
ADMIN_USER="${ADMIN_USER:-operator}"
REALM="${REALM:-homelab}"

CONTAINER="${KEYCLOAK_CONTAINER:-keycloak}"
SERVER="${KEYCLOAK_URL:-http://localhost:8282}"

CLIENT_ID="${CLIENT_ID:-ai-platform-datalake}"
# The client that *defines* the role, and whose name the datalake looks the role up
# under. Changing this means changing MCP_OIDC_CLIENT_ID in the datalake's .env too.
ROLE_CLIENT_ID="${ROLE_CLIENT_ID:-datalake-mcp}"
ROLE="${ROLE:-datalake:query}"

kc() { docker exec -i "$CONTAINER" /opt/keycloak/bin/kcadm.sh "$@"; }
uuid_of() {
  kc get clients -r "$REALM" -q "clientId=$1" --fields id --format csv --noquotes | tr -d '\r'
}

echo "Logging in to $SERVER as $ADMIN_USER in realm $ADMIN_REALM."
echo "Creating $CLIENT_ID in realm $REALM."
# No --password: kcadm prompts, so the password is never an argument.
docker exec -it "$CONTAINER" /opt/keycloak/bin/kcadm.sh config credentials \
  --server "$SERVER" --realm "$ADMIN_REALM" --user "$ADMIN_USER"

echo
ROLE_CLIENT_UUID=$(uuid_of "$ROLE_CLIENT_ID")
if [ -z "$ROLE_CLIENT_UUID" ]; then
  cat >&2 <<FAIL
FAIL  There is no client called $ROLE_CLIENT_ID in realm $REALM.

      That client is what defines the $ROLE role, and the datalake reads the role
      out of resource_access["$ROLE_CLIENT_ID"]. Nothing was created.

      Run scripts/setup-keycloak-mcp.sh first, then this script.
FAIL
  exit 1
fi

if ! kc get "clients/$ROLE_CLIENT_UUID/roles" -r "$REALM" --fields name --format csv --noquotes \
     | tr -d '\r' | grep -qx "$ROLE"; then
  cat >&2 <<FAIL
FAIL  $ROLE_CLIENT_ID exists but has no $ROLE role. Nothing was created.

      Run scripts/setup-keycloak-mcp.sh, which creates it.
FAIL
  exit 1
fi
echo "Found $ROLE on $ROLE_CLIENT_ID."

echo
echo "Creating client $CLIENT_ID..."
if [ -n "$(uuid_of "$CLIENT_ID")" ]; then
  echo "  already exists -- leaving its settings alone."
else
  kc create clients -r "$REALM" \
    -s "clientId=$CLIENT_ID" \
    -s 'name=AI platform -> datalake' \
    -s 'description=The ai.example.test gateway, reaching the datalake workbench API on a person'"'"'s behalf.' \
    -s 'protocol=openid-connect' \
    -s 'publicClient=false' \
    -s 'serviceAccountsEnabled=true' \
    -s 'standardFlowEnabled=false' \
    -s 'directAccessGrantsEnabled=false' \
    -s 'implicitFlowEnabled=false' \
    -s 'enabled=true' \
    -s 'fullScopeAllowed=true'
  echo "  created."
fi

CLIENT_UUID=$(uuid_of "$CLIENT_ID")

# fullScopeAllowed is set even on an existing client, because it is the setting that
# decides whether a role belonging to *another* client survives into this one's
# token. With it off, the grant below succeeds in the admin console and the token
# comes back with no resource_access entry at all -- which presents as an
# authorisation failure with nothing visibly wrong.
echo "Ensuring the token carries roles from other clients (fullScopeAllowed)..."
kc update "clients/$CLIENT_UUID" -r "$REALM" -s 'fullScopeAllowed=true'
echo "  done."

SA_USER=$(kc get "clients/$CLIENT_UUID/service-account-user" -r "$REALM" \
  --fields id --format csv --noquotes | tr -d '\r')

echo "Granting $ROLE_CLIENT_ID's $ROLE to $CLIENT_ID's service account..."
kc add-roles -r "$REALM" --uid "$SA_USER" --cclientid "$ROLE_CLIENT_ID" --rolename "$ROLE"
echo "  done."

echo
echo "Client secret for $CLIENT_ID -- put it in the ai-platform environment as"
echo "DATALAKE_OIDC_CLIENT_SECRET, and nowhere in a repository:"
SECRET=$(kc get "clients/$CLIENT_UUID/client-secret" -r "$REALM" --fields value --format csv --noquotes | tr -d '\r')
echo
echo "    $SECRET"

cat <<NEXT

Then set these three on the ai-platform api service and restart it:

    DATALAKE_API_URL=http://datalake-api:3444
    DATALAKE_OIDC_CLIENT_ID=$CLIENT_ID
    DATALAKE_OIDC_CLIENT_SECRET=<the secret above>

All three are required together. With any one missing the Data tab reports
"not configured" rather than failing once per request.

CHECK IT, from this host:

    TOKEN=\$(curl -s -X POST \\
      https://auth.example.test/realms/$REALM/protocol/openid-connect/token \\
      -d grant_type=client_credentials \\
      -d client_id=$CLIENT_ID \\
      -d client_secret='$SECRET' \\
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])')

The role must appear under $ROLE_CLIENT_ID, not under $CLIENT_ID. This is the whole
point of the script, so it is worth looking at:

    echo "\$TOKEN" | cut -d. -f2 \\
      | python3 -c 'import sys,base64,json; d=sys.stdin.read(); print(json.dumps(json.loads(base64.urlsafe_b64decode(d+"=="*3))["resource_access"], indent=2))'

    expect:  "$ROLE_CLIENT_ID": { "roles": [ "$ROLE" ] }

End to end against the API itself:

    docker exec datalake-api python -c "print('up')"
    docker run --rm --network primary_reseau curlimages/curl:latest -s \\
      -H "Authorization: Bearer \$TOKEN" http://datalake-api:3444/catalogue | head -c 300

Expect JSON with a "tables" array. Without the header the same request returns 401.
NEXT
