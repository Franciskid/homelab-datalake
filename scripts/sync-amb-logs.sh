#!/usr/bin/env bash
# Pull amblevilleserv's NPM logs to fraserv, where the pipeline runs.
#
# The camera host is a different machine (IMPLEMENTATION_PLAN.md section 2) and it
# produces most of the data: 28-115k lines a day against fraserv's 10-15k, because
# HLS streaming is thousands of segment requests per session.
#
# rsync rather than a second Dagster code location because it is one cron entry, one
# direction, and nothing new to run. The trade is real and worth stating: if this host
# is unreachable for longer than a rotation, that rotation is gone for good -- NPM keeps
# four weekly generations and no more.
#
# Nightly, before the 03:00 schedule:
#   30 2 * * * /home/francois/projects/homelab-datalake/scripts/sync-amb-logs.sh \
#                >> /home/francois/projects/homelab-datalake/logs/sync.log 2>&1
set -euo pipefail

REMOTE="${1:-sshamb.fraserv.fr}"
DEST="${2:-$(cd "$(dirname "$0")/.." && pwd)/logs/amb}"
CONTAINER="${NPM_CONTAINER:-npm-npm-1}"

mkdir -p "$DEST"

# NPM's logs live inside a container whose /data is this host path. Reading the bind
# mount directly avoids docker exec and keeps this a plain file copy.
REMOTE_DIR=$(ssh "$REMOTE" "docker inspect $CONTAINER \
  --format '{{range .Mounts}}{{if eq .Destination \"/data\"}}{{.Source}}{{end}}{{end}}'")

if [ -z "$REMOTE_DIR" ]; then
  echo "could not find the /data mount for $CONTAINER on $REMOTE" >&2
  exit 1
fi

echo "$(date -u +%FT%TZ) syncing $REMOTE:$REMOTE_DIR/logs -> $DEST"
rsync -az --delete-after \
  --include='*_access.log' \
  --include='*_access.log.*' \
  --exclude='*' \
  "$REMOTE:$REMOTE_DIR/logs/" "$DEST/"

echo "$(date -u +%FT%TZ) done: $(find "$DEST" -type f | wc -l) files, $(du -sh "$DEST" | cut -f1)"
