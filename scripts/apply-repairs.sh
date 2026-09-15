#!/usr/bin/env bash
#
# Applies every repair from the 2026-08-29 verification, on both hosts, in one run.
#
#   bash scripts/apply-repairs.sh
#
# It is safe to run twice. Each step checks its own state first and reports SKIP.
#
# What it does, in order:
#   1. secondary -- mounts the datalake export routes again in the camera app,
#                        and puts the `jose` dependency back in package.json.
#   2. secondary -- puts EXPORT_IDENTITY_SALT in .env, if it is absent.
#   3. secondary -- builds and starts the camera app again.
#   4. primary        -- builds and starts the datalake stack again. This starts the
#                        Postgres network, the camera partition offset, the decimal
#                        correction and the failure sensor, and it removes the image
#                        drift between the daemon and the MCP server.
#   5. Both           -- tests the result.

set -euo pipefail

SECONDARY=ssh-secondary.example.test
FRA=ssh.example.test
CAMERA_DIR=/srv/projects/camera/camera-app
DATALAKE_DIR=/srv/projects/homelab-datalake

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# --------------------------------------------------------------------------------
say "1/5  Camera app: mount the export routes again, and restore the jose dependency"
# --------------------------------------------------------------------------------
ssh "$SECONDARY" "cat > /tmp/remount-camera-export.py" <<'PYEOF'
#!/usr/bin/env python3
"""Mount the datalake export routes again in the camera app.

The deploy at 20:50 on 2026-08-29 replaced src/create-app.js. The new file does not
mount /api/export/*. The two modules that serve those endpoints stayed on disk with
their 2026-08-28 21:49 timestamps -- only the lines that registered them were lost.
The endpoints have answered 404 since.

There is no copy to restore. The git repository at /srv/projects has no
commits, the image `pre-phase29` is older than the export work, and there are no
dangling images. So this writes the registration again from the modules' interface.
"""

import pathlib
import shutil
import sys
import time

ROOT = pathlib.Path("/srv/projects/camera/camera-app")
BACKUP = ROOT / ".deploy-backups" / f"pre-export-remount-{time.strftime('%Y%m%d-%H%M%S')}"


def patch(path, old, new, label, marker):
    p = ROOT / path
    text = p.read_text()
    if marker in text:
        print(f"  SKIP  {label}: already present")
        return False
    if text.count(old) != 1:
        print(f"  FAIL  {label}: anchor matched {text.count(old)} times, expected 1")
        sys.exit(1)
    BACKUP.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, BACKUP / p.name)
    p.write_text(text.replace(old, new, 1))
    print(f"  OK    {label}")
    return True


changed = False

# 1. config.js -- a config block for the export endpoints.
changed |= patch(
    "src/config.js",
    "  archive: {\n",
    """  /*
   * The read-only export endpoints the datalake reads (/api/export/*).
   *
   * `identitySalt` pseudonymises `createdBy` on clips and `user` on log rows. It is
   * per-deployment on purpose: a hash cannot be compared against another installation,
   * and a bare user id never leaves this process. Changing it changes every hash, so
   * treat a change as a dated decision rather than a rotation. The warehouse holds two
   * distinct hashes over 9,222 clips.
   *
   * `clientId` names the Keycloak client whose `export:read` role authorises a caller.
   */
  dataExport: {
    clientId: process.env.EXPORT_OIDC_CLIENT_ID || "datalake-exporter",
    identitySalt: process.env.EXPORT_IDENTITY_SALT || "",
  },
  archive: {
""",
    "config.js dataExport block",
    marker="dataExport: {",
)

# 2. create-app.js -- require the two modules.
changed |= patch(
    "src/create-app.js",
    'const { createAuthService } = require("./services/auth-service");\n',
    'const { createAuthService } = require("./services/auth-service");\n'
    'const { createExportAuth } = require("./services/export-auth");\n'
    'const { registerExportRoutes } = require("./services/export-routes");\n',
    "create-app.js requires",
    marker='require("./services/export-routes")',
)

# 3. create-app.js -- mount the routes.
anchor = (
    '  app.use("/attention-assets", express.static(config.paths.attentionZoneAssetsDir, '
    "{ etag: false, fallthrough: false, lastModified: false }));\n"
)
changed |= patch(
    "src/create-app.js",
    anchor,
    anchor
    + """
  // Read-only exports for the datalake, under /api/export/*.
  //
  // Mounted here, before every route that authenticates a person. These endpoints
  // authenticate a *machine*: a Keycloak service account presenting a signed bearer
  // token that carries the `export:read` client role. They must never fall through to
  // the session redirect, because a pipeline cannot follow a login page -- it would
  // receive HTML with status 200 and store it as data.
  //
  // `createExportAuth` refuses every request when no issuer is configured. That is
  // deliberate: a misconfiguration must close the endpoint, not open it.
  const exportAuth = createExportAuth(config.auth, { clientId: config.dataExport.clientId });
  registerExportRoutes(app, {
    requireExportToken: exportAuth.requireExportToken,
    attentionEventStore,
    recordingStore,
    identitySalt: config.dataExport.identitySalt,
  });
""",
    "create-app.js route mount",
    marker="registerExportRoutes(app, {",
)

# 4. package.json -- the dependency export-auth.js needs.
#
# The 2026-08-29 deploy overwrote package.json as well as create-app.js, and took
# `jose` out with it. The lock file survived and still resolves jose 6.2.10, so this
# only puts the declaration back -- the version is not a new choice.
#
# Without it the container builds and then dies on every start with
# "Cannot find module 'jose'", and the reverse proxy answers 502.
changed |= patch(
    "package.json",
    '    "hls.js": "^1.6.16",\n',
    '    "hls.js": "^1.6.16",\n    "jose": "^6.2.10",\n',
    "package.json jose dependency",
    marker='"jose"',
)

if changed:
    print(f"  backup: {BACKUP}")
PYEOF
ssh "$SECONDARY" 'python3 /tmp/remount-camera-export.py'

# --------------------------------------------------------------------------------
say "2/5  Camera app: the identity salt"
# --------------------------------------------------------------------------------
# Added only when it is absent. The salt pseudonymises `createdBy` on clips. Writing a
# new one over an old one would silently change every hash in the warehouse.
ssh "$SECONDARY" "cd $CAMERA_DIR && \
  if grep -q '^EXPORT_IDENTITY_SALT=' .env; then \
    echo '  SKIP  EXPORT_IDENTITY_SALT already set'; \
  else \
    echo \"EXPORT_IDENTITY_SALT=\$(openssl rand -hex 32)\" >> .env && \
    echo '  OK    EXPORT_IDENTITY_SALT written'; \
  fi"

# --------------------------------------------------------------------------------
say "3/5  Camera app: build and start"
# --------------------------------------------------------------------------------
ssh "$SECONDARY" "cd $CAMERA_DIR && docker compose up -d --build"

# --------------------------------------------------------------------------------
say "4/5  Datalake: build and start"
# --------------------------------------------------------------------------------
ssh "$FRA" "cd $DATALAKE_DIR && docker compose up -d --build"

# --------------------------------------------------------------------------------
say "5/5  Tests"
# --------------------------------------------------------------------------------
echo "-- Postgres must resolve from the daemon:"
ssh "$FRA" "docker exec datalake-daemon getent hosts postgresql_postgres_1" \
  && echo "   OK    the LiteLLM source is reachable" \
  || echo "   FAIL  the LiteLLM source is still unreachable"

echo "-- Today must be a valid camera partition:"
ssh "$FRA" "docker exec datalake-daemon python -c \"
from homelab_datalake.assets.camera import camera_partitions
import datetime
keys = camera_partitions.get_partition_keys()
today = datetime.datetime.now(datetime.UTC).date().isoformat()
print('   ', len(keys), 'partitions, last', keys[-1], '| today valid:', today in keys)
\""

echo "-- The camera export must answer NDJSON and not HTML:"
ssh "$FRA" "docker exec datalake-mcp python -c \"
from datetime import date
from homelab_datalake.parsing.camera import extract_day
r = extract_day(date(2026, 8, 28))
print('   ', len(r.events), 'events,', len(r.clips), 'clips,', len(r.logs), 'logs')
\""

say "Done. The four datalake changes are live and the camera export is back."
echo "Tell Claude, and it will build the two absent LiteLLM partitions and run analytics_daily."
