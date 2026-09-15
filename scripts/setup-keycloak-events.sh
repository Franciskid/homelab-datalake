#!/usr/bin/env bash
# Turn on Keycloak's event store and let the datalake read it. Run once, on primary.
#
#   bash scripts/setup-keycloak-events.sh            # show what it would do
#   bash scripts/setup-keycloak-events.sh --apply    # do it
#
# It asks for the Keycloak administrator password interactively, so nothing it prints
# and nothing in your shell history contains one -- the same as the other
# setup-keycloak-*.sh scripts.
#
# WHY THIS EXISTS
#
# On 2026-09-15 both realms had events switched off and `event_entity` was empty, so
# there was no record of who signed in to Grafana, Dagster, Jellyfin or the AI platform,
# or of any failed attempt. Keycloak only records events from the moment they are
# enabled: nothing before that can ever be recovered.
#
# WHAT IT CHANGES
#
#   1. In realms `homelab` and `master`: user events on (Keycloak's default set --
#      logins, logouts, code-to-token, errors), kept 90 days; admin events on, WITHOUT
#      representation details. A client's representation includes its secret, and the
#      datalake must never receive one.
#   2. On the keycloak database: column-level SELECT for the existing read-only role
#      `datalake_ro` (infra/keycloak-datalake-grants.sql, generated from the code that
#      reads it). No new role, no new password.
#
# The lake keeps every event after Keycloak expires it (keycloak_raw merges by id).

set -euo pipefail

MODE="plan"
[[ "${1:-}" == "--apply" ]] && MODE="apply"

ADMIN_REALM="${ADMIN_REALM:-master}"
ADMIN_USER="${ADMIN_USER:-operator}"
REALMS="${REALMS:-homelab master}"
EXPIRATION_SECONDS="${EXPIRATION_SECONDS:-7776000}"   # 90 days

CONTAINER="${KEYCLOAK_CONTAINER:-keycloak}"
SERVER="${KEYCLOAK_URL:-http://localhost:8282}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgresql_postgres_1}"
GRANTS="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/infra/keycloak-datalake-grants.sql"

if [[ ! -f "$GRANTS" ]]; then
  echo "FAIL  $GRANTS is missing; run this from the deployed homelab-datalake directory." >&2
  exit 1
fi

echo "Realms:       $REALMS"
echo "User events:  on, default types, expire after $((EXPIRATION_SECONDS / 86400)) days"
echo "Admin events: on, representation details OFF (a client representation holds its secret)"
echo "Grants:       $GRANTS -> database keycloak, role datalake_ro"
echo
if [[ "$MODE" != "apply" ]]; then
  echo "Nothing changed. Re-run with --apply."
  exit 0
fi

kc() { docker exec -i "$CONTAINER" /opt/keycloak/bin/kcadm.sh "$@"; }

echo "Logging in to $SERVER as $ADMIN_USER in realm $ADMIN_REALM."
# No --password: kcadm prompts, so the password is never an argument.
docker exec -it "$CONTAINER" /opt/keycloak/bin/kcadm.sh config credentials \
  --server "$SERVER" --realm "$ADMIN_REALM" --user "$ADMIN_USER"

for realm in $REALMS; do
  kc update "realms/$realm" \
    -s eventsEnabled=true \
    -s "eventsExpiration=$EXPIRATION_SECONDS" \
    -s adminEventsEnabled=true \
    -s adminEventsDetailsEnabled=false
  echo "  OK    $realm: $(kc get "realms/$realm" --fields eventsEnabled,eventsExpiration,adminEventsEnabled,adminEventsDetailsEnabled --format csv --noquotes | tr -d '\r')"
done

docker exec -i "$POSTGRES_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d keycloak < "$GRANTS"
echo "  OK    grants applied"

# The admin CLI keeps a refresh token in the container. Remove it, so the session this
# script opened does not outlive the script.
docker exec "$CONTAINER" rm -f /opt/keycloak/.keycloak/kcadm.config
# Only now is there anything to read, so only now does the hourly schedule start. Started
# earlier it would fail every hour on "permission denied" and open an incident each time.
docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon \
  dagster schedule start keycloak_schedule -w /app/workspace.yaml --location homelab_datalake.definitions
echo "  OK    keycloak_schedule started"
echo
echo "Done. keycloak_raw lands events from the next hourly run (at :35)."
