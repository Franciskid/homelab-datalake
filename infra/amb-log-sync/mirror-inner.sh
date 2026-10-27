#!/bin/sh
# Runs INSIDE the disposable minio/mc container -- see sync-amb-logs.sh, which
# bind-mounts this file in and sets it as the entrypoint. Not meant to run
# anywhere else: it assumes /logs is amblevilleserv's NPM log directory (read-only)
# and that an `mc` alias named "lake" is already configured via MC_HOST_lake.
#
# One `mc cp` per file, not `mc mirror`. `mc mirror` compares source against
# destination, which means listing the destination -- and the key this container
# authenticates as can only PutObject on inbox/npm/amb/* (see minio-policy.json):
# no ListBucket, no GetObject, no DeleteObject. A plain `cp` per file needs none of
# that. Which files changed is decided outside, by sync-amb-logs.sh, and passed as
# arguments; with no arguments every log is uploaded.
#
# ash/dash-compatible on purpose: this is the shell minio/mc's image actually has.
set -eu

bucket="${AMB_LOG_SYNC_BUCKET:?AMB_LOG_SYNC_BUCKET is not set}"
prefix="inbox/npm/amb"

count=0
# Same two shapes homelab_datalake.parsing.extract.ACCESS_LOG_GLOBS reads on the
# datalake side: the live file and every rotated generation, `.gz` included. A
# glob that matches nothing expands to itself when nullglob is unavailable in this
# shell, hence the `[ -e "$f" ]` guard rather than trusting the loop to skip it.
if [ "$#" -eq 0 ]; then
    set -- /logs/*_access.log /logs/*_access.log.*
fi

for f in "$@"; do
    case "$f" in /*) ;; *) f="/logs/$f" ;; esac
    [ -e "$f" ] || continue
    name=$(basename "$f")
    case "$name" in
        *_error.log*) continue ;;
    esac
    mc cp --quiet "$f" "lake/${bucket}/${prefix}/${name}"
    count=$((count + 1))
done

if [ "$count" -eq 0 ]; then
    echo "amb-log-sync: no access logs found under /logs -- check the bind mount" >&2
    exit 1
fi

echo "amb-log-sync: uploaded ${count} file(s) to s3://${bucket}/${prefix}/"
