#!/usr/bin/env bash
#
# Registers the datalake MCP server as an upstream on the LiteLLM MCP gateway.
#
#     bash scripts/register-mcp-gateway.sh
#
# Run from a clone on your own machine. It is idempotent.
#
# WHY THE GATEWAY NEEDS A KEYCLOAK CLIENT AND NOT A SHARED TOKEN
# The other three upstreams use a static bearer, or none at all because they are bound
# to loopback. The datalake is neither: it is reachable from the public internet and it
# verifies a Keycloak signature on every request. A static token cannot work, because a
# Keycloak access token lives for minutes.
#
# LiteLLM can do the client_credentials exchange itself, so no refresher sidecar is
# needed. Two details decide whether it actually does:
#
#   * `token_url` must be given. LiteLLM does not read the server's
#     /.well-known/oauth-protected-resource, even though the datalake publishes it.
#   * `oauth2_flow: client_credentials` must be set. Without it LiteLLM treats the
#     server as interactive per-user OAuth and never mints a token at all. Presence of
#     client_id/client_secret/token_url is deliberately NOT enough -- auto-detecting
#     machine-to-machine from those fields was a regression they reverted.

set -euo pipefail

FRA=ssh.example.test
AI=/srv/projects/ai-platform/deployment
AI_ROOT=/srv/projects/ai-platform
# The platform reads .env.production, not .env, and passes it to compose with
# --env-file. scripts/prod-stack.sh is the canonical invocation; this follows it
# rather than inventing a second way to start the same stack.
ENV_FILE="$AI/.env.production"
CLIENT_UUID=2dd51156-70fc-5772-8dcc-497e2a953b67

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

say "1/5  Keycloak session"
if ! ssh "$FRA" "docker exec keycloak /opt/keycloak/bin/kcadm.sh get clients/$CLIENT_UUID -r homelab --fields clientId" >/dev/null 2>&1; then
  echo "  The kcadm session has expired. Log in again, then run this script:"
  echo
  echo "    ssh -t $FRA 'docker exec -it keycloak /opt/keycloak/bin/kcadm.sh config credentials \\"
  echo "        --server http://localhost:8282 --realm master --user operator'"
  exit 1
fi
echo "  OK    session is live"

say "2/5  Secret into the ai-platform environment"
# The secret is read and written entirely on the server. It is never printed and never
# leaves that host.
ssh "$FRA" "set -e
  test -f '$ENV_FILE' || { echo '  no env file at $ENV_FILE'; exit 1; }
  S=\$(docker exec keycloak /opt/keycloak/bin/kcadm.sh get clients/$CLIENT_UUID/client-secret \
        -r homelab --fields value --format csv --noquotes | tr -d '\r\n')
  test -n \"\$S\" || { echo '  could not read the client secret'; exit 1; }
  cp '$ENV_FILE' '$ENV_FILE.bak-datalake-mcp-\$(date +%Y%m%d-%H%M%S)'
  if grep -q '^DATALAKE_MCP_CLIENT_SECRET=' '$ENV_FILE'; then
    sed -i \"s|^DATALAKE_MCP_CLIENT_SECRET=.*|DATALAKE_MCP_CLIENT_SECRET=\$S|\" '$ENV_FILE'
    echo '  OK    secret updated'
  else
    printf '\n# The homelab datalake, registered as an upstream on the MCP gateway.\n# Keycloak client_credentials: the gateway mints its own short-lived token.\nDATALAKE_MCP_URL=https://data.example.test/mcp\nDATALAKE_MCP_CLIENT_SECRET=%s\n' \"\$S\" >> '$ENV_FILE'
    echo '  OK    secret written'
  fi
  grep -q '^DATALAKE_MCP_URL=' '$ENV_FILE' || echo 'DATALAKE_MCP_URL=https://data.example.test/mcp' >> '$ENV_FILE'
  grep -oE '^DATALAKE_MCP_[A-Z_]+=' '$ENV_FILE' | sed 's/^/  /'"

say "3/5  The mcp_servers entry"
ssh "$FRA" "python3 - <<'PYEOF'
import pathlib, sys
p = pathlib.Path('$AI/litellm/config.yaml')
t = p.read_text()
if 'datalake:' in t:
    print('  SKIP  the datalake entry is already there'); sys.exit(0)
anchor = '\nlitellm_settings:'
if t.count(anchor) != 1:
    print('  FAIL  could not find litellm_settings'); sys.exit(1)
entry = '''  # The homelab datalake: typed analytical tools over the warehouse. Web traffic,
  # LLM spend, camera activity and the media library, behind a semantic layer -- no
  # tool takes SQL.
  #
  # Unlike media and finance this one is not loopback-bound, so it authenticates. It
  # verifies a Keycloak signature per request, which rules out a static bearer: the
  # token lives for minutes. LiteLLM does the client_credentials exchange itself.
  #
  # oauth2_flow is load-bearing. Without it LiteLLM classes this as interactive
  # per-user OAuth and never mints a token, and the tools silently do not appear.
  datalake:
    url: os.environ/DATALAKE_MCP_URL
    transport: http
    description: Homelab warehouse analytics -- web traffic through the reverse proxy,
      LLM spend by model and workload, camera activity, and the Jellyfin library.
    auth_type: oauth2
    oauth2_flow: client_credentials
    client_id: datalake-mcp
    client_secret: os.environ/DATALAKE_MCP_CLIENT_SECRET
    token_url: https://auth.example.test/realms/homelab/protocol/openid-connect/token
    allow_all_keys: true
'''
p.write_text(t.replace(anchor, '\n' + entry + anchor, 1))
print('  OK    datalake entry added')
PYEOF"

say "4/5  Pass the two variables to the container"
ssh "$FRA" "python3 - <<'PYEOF'
import pathlib, sys
p = pathlib.Path('$AI/compose.prod.yml')
t = p.read_text()
if 'DATALAKE_MCP_URL' in t:
    print('  SKIP  already passed through'); sys.exit(0)
anchor = '      FINANCE_MCP_URL: \${FINANCE_MCP_URL:-http://127.0.0.1:4022/mcp}\n'
if t.count(anchor) != 1:
    print('  FAIL  could not find FINANCE_MCP_URL'); sys.exit(1)
add = ('      DATALAKE_MCP_URL: \${DATALAKE_MCP_URL:-}\n'
       '      DATALAKE_MCP_CLIENT_SECRET: \${DATALAKE_MCP_CLIENT_SECRET:-}\n')
p.write_text(t.replace(anchor, anchor + add, 1))
print('  OK    compose.prod.yml updated')
PYEOF"

say "5/5  Restart LiteLLM"
ssh "$FRA" "PLATFORM_ENV_FILE=$ENV_FILE docker compose --env-file $ENV_FILE \
  -f $AI/compose.prod.yml up -d litellm"
echo
echo "Give it a few seconds, then tell Claude. It will check the gateway lists the datalake tools."
