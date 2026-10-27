#!/usr/bin/env bash
# Mirrors amblevilleserv's own NPM access logs into the lake's inbox, hourly.
#
# Runs ON amblevilleserv, from cron -- see README.md for the exact line and for
# how the write-only key this script uses gets created. Nothing is installed on
# the host: every run starts a disposable `minio/mc` container, uses it once for
# the upload, and removes it. The NPM log directory is bind-mounted read-only,
# so a bug here can upload the wrong bytes but cannot corrupt or delete a log
# nginx is still writing to -- the same posture docker-compose.yml already takes
# for the datalake containers reading fraserv's own NPM logs.
#
# WS-41.E2 part B: this exists because amblevilleserv cannot be mounted into the
# datalake containers directly (different host, no shared filesystem), so its
# logs reach the lake by mirror instead -- see homelab_datalake.assets.npm_access
# for the read side (extract_partition_from_lake).
set -euo pipefail

LOG_DIR="${AMB_NPM_LOG_DIR:-/home/docker/npm/data/logs}"
CREDENTIALS_FILE="${AMB_LOG_SYNC_CREDENTIALS_FILE:-/etc/amb-log-sync/credentials.env}"
ENDPOINT="${AMB_LOG_SYNC_ENDPOINT:-https://s3.homelab.fraserv.fr}"
BUCKET="${AMB_LOG_SYNC_BUCKET:-datalake}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# What the last successful run uploaded, one `sha256sum` line per file.
STATE_FILE="${AMB_LOG_SYNC_STATE_FILE:-$SCRIPT_DIR/uploaded.sha256}"

if [[ ! -d "$LOG_DIR" ]]; then
  echo "amb-log-sync: $LOG_DIR not found" >&2
  exit 1
fi

# The key's own credentials, never on the command line and never in this script.
# root-only file, one PutObject-scoped key inside -- see README.md for how it is
# provisioned and installed. A world-readable or missing file is refused rather
# than falling through to an empty credential, which `mc` would treat as
# anonymous and fail on with a confusing 403 instead of this clear message.
if [[ ! -r "$CREDENTIALS_FILE" ]]; then
  echo "amb-log-sync: cannot read $CREDENTIALS_FILE -- see README.md 'Provisioning'" >&2
  exit 1
fi
perms=$(stat -c '%a' "$CREDENTIALS_FILE" 2>/dev/null || stat -f '%Lp' "$CREDENTIALS_FILE")
if [[ "$perms" != "600" && "$perms" != "400" ]]; then
  echo "amb-log-sync: $CREDENTIALS_FILE is mode $perms, expected 600 (chmod 600 it)" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$CREDENTIALS_FILE"   # sets AMB_LOG_SYNC_ACCESS_KEY / AMB_LOG_SYNC_SECRET_KEY
: "${AMB_LOG_SYNC_ACCESS_KEY:?not set in $CREDENTIALS_FILE}"
: "${AMB_LOG_SYNC_SECRET_KEY:?not set in $CREDENTIALS_FILE}"

# Only the files whose bytes changed since the last successful run. The bucket is
# versioned, so re-sending an unchanged rotated log every hour kept a copy of it
# each time -- 45 GB of identical NPM logs by 27 Oct 2026. The key cannot read the
# bucket to compare against it, so the comparison is against what this host last
# sent. Losing the state file costs one full upload, nothing more.
touch "$STATE_FILE"
current=()
changed=()
for f in "$LOG_DIR"/*_access.log "$LOG_DIR"/*_access.log.*; do
  [[ -e "$f" ]] || continue
  line=$(sha256sum "$f")
  current+=("$line")
  grep -qxF -- "$line" "$STATE_FILE" || changed+=("$(basename "$f")")
done

if [[ ${#current[@]} -eq 0 ]]; then
  echo "amb-log-sync: no access logs found under $LOG_DIR -- check AMB_NPM_LOG_DIR" >&2
  exit 1
fi
if [[ ${#changed[@]} -eq 0 ]]; then
  echo "amb-log-sync: ${#current[@]} file(s), none changed since the last upload"
  exit 0
fi

# mc's non-interactive alias form: MC_HOST_<alias>=https://KEY:SECRET@host. Never
# passed as a CLI argument (so never in `ps`), never echoed, never written to a
# file inside the container -- it lives only in this one environment variable,
# which docker sends straight to the process and drops when the container exits.
docker run --rm \
  -v "$LOG_DIR:/logs:ro" \
  -v "$SCRIPT_DIR/mirror-inner.sh:/mirror-inner.sh:ro" \
  -e "MC_HOST_lake=https://${AMB_LOG_SYNC_ACCESS_KEY}:${AMB_LOG_SYNC_SECRET_KEY}@${ENDPOINT#https://}" \
  -e "AMB_LOG_SYNC_BUCKET=${BUCKET}" \
  --entrypoint sh \
  minio/mc:latest /mirror-inner.sh "${changed[@]}"

# Recorded only after every upload succeeded (set -e stops before this otherwise),
# so a failed run is retried in full next hour.
printf '%s\n' "${current[@]}" > "$STATE_FILE"
