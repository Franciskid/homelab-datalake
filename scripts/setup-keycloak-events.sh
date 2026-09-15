#!/usr/bin/env bash
# Turn on Keycloak's event store and let the datalake read it. Run once, on primary.
#
#   bash scripts/setup-keycloak-events.sh            # show what it would do
#   bash scripts/setup-keycloak-events.sh --apply    # do it
#
# WHERE THE ADMINISTRATOR PASSWORD COMES FROM
#
# In this order: KEYCLOAK_ADMIN_USER / KEYCLOAK_ADMIN_PASSWORD in the environment; the
# administrator the keycloak container itself was started with (KC_BOOTSTRAP_ADMIN_*,
# already on this host, and how the realms were configured in the first place); a
# prompt. Whichever it is, it reaches the admin API on stdin: never an argument, so
# never in `ps`, never echoed and never written to disk. Same handling as
# scripts/enable-lake-versioning.sh does for the MinIO root key, and the reason this
# one can be run unattended.
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

# --- the administrator ------------------------------------------------------------
ADMIN_USER="${KEYCLOAK_ADMIN_USER:-}"
ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-}"
# A user and a password belong together, so the container supplies both or neither.
if [[ -z "$ADMIN_USER" && -z "$ADMIN_PASSWORD" ]] && docker inspect "$CONTAINER" >/dev/null 2>&1; then
  env_lines=$(docker inspect "$CONTAINER" --format '{{range .Config.Env}}{{println .}}{{end}}')
  ADMIN_USER=$(grep -m1 '^KC_BOOTSTRAP_ADMIN_USERNAME=' <<<"$env_lines" | cut -d= -f2- || true)
  ADMIN_PASSWORD=$(grep -m1 '^KC_BOOTSTRAP_ADMIN_PASSWORD=' <<<"$env_lines" | cut -d= -f2- || true)
  unset env_lines
  if [[ -n "$ADMIN_USER" && -n "$ADMIN_PASSWORD" ]]; then
    echo "Administrator: $ADMIN_USER, as the $CONTAINER container was started with it."
  else
    ADMIN_USER=""
    ADMIN_PASSWORD=""
  fi
fi
if [[ -z "$ADMIN_USER" ]]; then
  read -rp "Keycloak admin user: " ADMIN_USER
fi
if [[ -z "$ADMIN_PASSWORD" ]]; then
  read -rsp "Keycloak admin password: " ADMIN_PASSWORD
  echo
fi
if [[ -z "$ADMIN_USER" || -z "$ADMIN_PASSWORD" ]]; then
  echo "FAIL  no administrator credentials. Nothing was changed." >&2
  exit 1
fi

# --- the realms -------------------------------------------------------------------
#
# A file rather than a program piped in, so the credentials have stdin to themselves.
JOB=$(mktemp)
trap 'rm -f "$JOB"' EXIT
cat > "$JOB" <<'PYEOF'
"""Switch the event store on in each realm, through the admin API."""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

# The credentials arrive on stdin and nowhere else: not argv, not the environment.
user = sys.stdin.readline().rstrip("\n")
password = sys.stdin.readline().rstrip("\n")
if not user or not password:
    raise SystemExit("no credentials arrived on stdin")

SERVER = os.environ["KEYCLOAK_SERVER"].rstrip("/")
ADMIN_REALM = os.environ["KEYCLOAK_ADMIN_REALM"]
REALMS = os.environ["KEYCLOAK_REALMS"].split()
APPLY = os.environ.get("SETUP_MODE") == "apply"

WANTED = {
    "eventsEnabled": True,
    "eventsExpiration": int(os.environ["EXPIRATION_SECONDS"]),
    "adminEventsEnabled": True,
    # Representation details hold a client's secret. The datalake must never see one.
    "adminEventsDetailsEnabled": False,
}


def call(method, path, token, body=None):
    headers = {"Authorization": f"Bearer {token}"}
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(SERVER + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace")[:400]
        raise SystemExit(f"FAIL  {method} {path} -> {err.code} {detail}") from None
    return json.loads(raw) if raw else None


form = urllib.parse.urlencode(
    {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": user,
        "password": password,
    }
).encode()
req = urllib.request.Request(
    f"{SERVER}/realms/{ADMIN_REALM}/protocol/openid-connect/token",
    data=form,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    method="POST",
)
try:
    with urllib.request.urlopen(req) as resp:
        token = json.load(resp)["access_token"]
except urllib.error.HTTPError as err:
    detail = err.read().decode(errors="replace")[:300]
    print(
        f"FAIL  cannot sign in as {user!r} in realm {ADMIN_REALM}: {err.code} {detail}",
        file=sys.stderr,
    )
    # 3 means "the credentials were refused", which is worth a second try with
    # different ones rather than a stack trace.
    raise SystemExit(3) from None
finally:
    del password, form

print(f"Signed in to {SERVER} as {user} in realm {ADMIN_REALM}.")
print()

# The events config is its own small representation. Read it, change the four fields
# that matter and write the same shape back, so nothing else in the realm is touched --
# in particular eventsListeners and enabledEventTypes keep whatever they hold.
for realm in REALMS:
    path = f"/admin/realms/{realm}/events/config"
    config = call("GET", path, token)
    before = {key: config.get(key) for key in WANTED}
    if before == WANTED:
        print(f"  OK    {realm}: already on, {before['eventsExpiration'] // 86400} day expiry")
        continue
    if not APPLY:
        print(f"  would {realm}: {before} -> {WANTED}")
        continue
    config.update(WANTED)
    call("PUT", path, token, config)
    after = {key: call("GET", path, token).get(key) for key in WANTED}
    if after != WANTED:
        raise SystemExit(f"FAIL  {realm} reads back as {after}")
    print(f"  OK    {realm}: {after}")

if not APPLY:
    print()
    print("Nothing changed. Re-run with --apply.")
PYEOF

run_job() {
  printf '%s\n%s\n' "$1" "$2" \
    | SETUP_MODE="$MODE" \
      KEYCLOAK_SERVER="$SERVER" \
      KEYCLOAK_ADMIN_REALM="$ADMIN_REALM" \
      KEYCLOAK_REALMS="$REALMS" \
      EXPIRATION_SECONDS="$EXPIRATION_SECONDS" \
      python3 "$JOB"
}

set +e
run_job "$ADMIN_USER" "$ADMIN_PASSWORD"
status=$?
set -e

# Exit 3 is "Keycloak refused those credentials". The ones found in the container are a
# guess -- on this host KC_BOOTSTRAP_ADMIN_* still names an account that was removed once
# a real administrator existed -- so refusal means ask, not give up.
if [[ $status -eq 3 ]]; then
  if [[ ! -t 0 ]]; then
    echo "FAIL  no working administrator and no terminal to ask on. Run this from a shell," >&2
    echo "      or pass KEYCLOAK_ADMIN_USER and KEYCLOAK_ADMIN_PASSWORD. Nothing changed." >&2
    exit 3
  fi
  echo
  read -rp "Keycloak admin user [${ADMIN_USER}]: " typed_user
  ADMIN_USER="${typed_user:-$ADMIN_USER}"
  read -rsp "Keycloak admin password: " ADMIN_PASSWORD
  echo
  echo
  set +e
  run_job "$ADMIN_USER" "$ADMIN_PASSWORD"
  status=$?
  set -e
fi
unset ADMIN_PASSWORD
[[ $status -eq 0 ]] || exit $status

if [[ "$MODE" != "apply" ]]; then
  echo "  would apply $GRANTS and start keycloak_schedule."
  exit 0
fi

docker exec -i "$POSTGRES_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d keycloak < "$GRANTS"
echo "  OK    grants applied"

# Only now is there anything to read, so only now does the hourly schedule start. Started
# earlier it would fail every hour on "permission denied" and open an incident each time.
docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon \
  dagster schedule start keycloak_schedule -w /app/workspace.yaml --location homelab_datalake.definitions
echo "  OK    keycloak_schedule started"
echo
echo "Done. keycloak_raw lands events from the next hourly run (at :35)."
