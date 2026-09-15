#!/bin/sh
# Runs INSIDE the disposable minio/mc container -- see sync-secondary-logs.sh, which
# bind-mounts this file in and sets it as the entrypoint. Not meant to run
# anywhere else: it assumes /logs is secondary's NPM log directory (read-only)
# and that an `mc` alias named "lake" is already configured via MC_HOST_lake.
#
# One `mc cp` per file, not `mc mirror`. `mc mirror` compares source against
# destination, which means listing the destination -- and the key this container
# authenticates as can only PutObject on inbox/npm/secondary/* (see minio-policy.json):
# no ListBucket, no GetObject, no DeleteObject. A plain `cp` per file needs none of
# that; the trade is re-uploading everything every run rather than only what
# changed, which at secondary's log volume (tens of MB) costs seconds, not minutes.
#
# ash/dash-compatible on purpose: this is the shell minio/mc's image actually has.
set -eu

bucket="${SECONDARY_LOG_SYNC_BUCKET:?SECONDARY_LOG_SYNC_BUCKET is not set}"
prefix="inbox/npm/secondary"

count=0
# Same two shapes homelab_datalake.parsing.extract.ACCESS_LOG_GLOBS reads on the
# datalake side: the live file and every rotated generation, `.gz` included. A
# glob that matches nothing expands to itself when nullglob is unavailable in this
# shell, hence the `[ -e "$f" ]` guard rather than trusting the loop to skip it.
for f in /logs/*_access.log /logs/*_access.log.*; do
    [ -e "$f" ] || continue
    name=$(basename "$f")
    case "$name" in
        *_error.log*) continue ;;
    esac
    mc cp --quiet "$f" "lake/${bucket}/${prefix}/${name}"
    count=$((count + 1))
done

if [ "$count" -eq 0 ]; then
    echo "secondary-log-sync: no access logs found under /logs -- check the bind mount" >&2
    exit 1
fi

echo "secondary-log-sync: uploaded ${count} file(s) to s3://${bucket}/${prefix}/"
