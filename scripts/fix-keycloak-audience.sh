#!/usr/bin/env bash
# Put the client's own id into its access-token audience.
#
# THE BUG THIS FIXES
#
# Logging in to data.example.test returned a 500 from oauth2-proxy:
#
#   Error creating session during OAuth2 callback: audience from claim aud with value
#   [banking-tracker-web primary-web datalake-mcp account] does not match with any of
#   allowed audiences map[datalake-web:{}]
#
# oauth2-proxy requires its own client id in the token audience. Keycloak does not put
# it there: an access token's `aud` carries only the audiences *resolved from the
# user's client roles*, plus `account`. Authorisation for the Dagster UI is a realm
# role (datalake-user), not a client role, so `datalake-web` never appeared -- and the
# check could never pass.
#
# It stayed hidden because an existing session cookie kept working. The failure only
# surfaced on the first fresh login after that cookie expired, which is why it looked
# like a sudden outage rather than a configuration gap that had been there all along.
#
# The fix is a dedicated audience mapper, which is Keycloak's supported way of saying
# "this token is for this client". Nothing is loosened: the alternative -- telling
# oauth2-proxy to also accept `account` -- would accept any token issued to any client
# in the realm.

set -euo pipefail

ADMIN_REALM="${ADMIN_REALM:-master}"
ADMIN_USER="${ADMIN_USER:-operator}"
REALM="${REALM:-homelab}"
CONTAINER="${KEYCLOAK_CONTAINER:-keycloak}"
SERVER="${KEYCLOAK_URL:-http://localhost:8282}"

# Every client that is protected by something checking the audience.
CLIENTS="${CLIENTS:-datalake-web}"

kc() { docker exec -i "$CONTAINER" /opt/keycloak/bin/kcadm.sh "$@"; }

echo "Logging in to $SERVER as $ADMIN_USER in realm $ADMIN_REALM."
docker exec -it "$CONTAINER" /opt/keycloak/bin/kcadm.sh config credentials \
  --server "$SERVER" --realm "$ADMIN_REALM" --user "$ADMIN_USER"

for CLIENT_ID in $CLIENTS; do
  UUID=$(kc get clients -r "$REALM" -q "clientId=$CLIENT_ID" --fields id --format csv --noquotes | tr -d '\r')
  if [ -z "$UUID" ]; then echo "  no client $CLIENT_ID in $REALM, skipping"; continue; fi

  if kc get "clients/$UUID/protocol-mappers/models" -r "$REALM" --fields name --format csv --noquotes 2>/dev/null | grep -qx "audience"; then
    echo "  $CLIENT_ID already has an audience mapper."
    continue
  fi

  echo "  adding an audience mapper to $CLIENT_ID"
  kc create "clients/$UUID/protocol-mappers/models" -r "$REALM" \
    -s 'name=audience' \
    -s 'protocol=openid-connect' \
    -s 'protocolMapper=oidc-audience-mapper' \
    -s "config.\"included.client.audience\"=$CLIENT_ID" \
    -s 'config."access.token.claim"=true' \
    -s 'config."id.token.claim"=false'
  echo "    done."
done

cat <<'NEXT'

Now sign out and sign in again at https://data.example.test -- the old session cookie
is what was masking this, so a stale one will keep the old behaviour.

If it still fails, the exact reason is one line in:

  docker logs datalake-oauth2-proxy | grep -i audience
NEXT
