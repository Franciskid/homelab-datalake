# amb log sync

Mirrors amblevilleserv's own Nginx Proxy Manager logs into the lake bucket, hourly,
so `homelab_datalake.assets.npm_access.npm_access_raw` can read amb's traffic the
same way it always has fraserv's -- just sourced from `inbox/npm/amb/` in the lake
instead of a local mount. See that module's docstring, and PLAN.md, for why: the
datalake containers run on fraserv and cannot reach amblevilleserv directly, so
`NPM_LOG_DIR_AMB` used to name a path (`/logs/amb`) that existed in no container at
all, and every amb partition read an empty directory in silence. WS-41.E2 part B.

## What runs where

| Host | What |
| --- | --- |
| amblevilleserv | `sync-amb-logs.sh` + `mirror-inner.sh`, via cron, hourly |
| fraserv | Nothing new. `npm_access_raw` reads the inbox through the `lake` resource it already has. |

`sync-amb-logs.sh` starts a disposable `minio/mc` container for each run and
removes it afterwards -- nothing is installed on amblevilleserv itself. The NPM
log directory is bind-mounted **read-only**; the container can upload from it but
cannot write back into it.

Each file is uploaded with a plain `mc cp`, not `mc mirror`: the key this script
authenticates as can only `PutObject` under `inbox/npm/amb/*` (see
`minio-policy.json`) -- no `ListBucket`, no `GetObject`, no `DeleteObject`, on
that prefix or anywhere else in the bucket, which `mc mirror`'s change-detection
would need. The whole log directory re-uploads every run instead of only what
changed; at amb's volume (tens of MB, one virtual host) that costs seconds.

`parsing.extract.extract_partition_from_lake` and its parser are unaffected by a
file being re-uploaded unchanged: the partition is defined by each line's own
timestamp, never by the file or when it last changed (see `parsing.extract`'s
module docstring), and rejects are deduplicated by their exact text. Re-reading
the same bytes every hour is exactly as safe as reading them once.

## Provisioning (run once, by whoever has fraserv's MinIO root credentials)

The key is scoped to nothing but `PutObject` on `inbox/npm/amb/*` -- see
`minio-policy.json`. A compromised amblevilleserv can flood that one prefix; it
cannot read, list or delete a single object anywhere else in the bucket.

```bash
# 1. On fraserv, with the MinIO root credentials available (see
#    scripts/enable-lake-versioning.sh for how that script sources them from the
#    homelab-minio container's own environment rather than typing them anywhere).
#    Runs mc from a disposable container against the running server -- nothing
#    installed on fraserv either.

MINIO_ROOT_USER=...      # from the homelab-minio container's env
MINIO_ROOT_PASSWORD=...
MINIO_NETWORK=$(docker inspect homelab-minio \
  --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' | head -1)

docker run --rm -i \
  --network "$MINIO_NETWORK" \
  -v "$(pwd)/infra/amb-log-sync/minio-policy.json:/tmp/policy.json:ro" \
  -e MC_HOST_root="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@homelab-minio:9000" \
  minio/mc:latest sh -s <<'EOF'
set -eu
mc admin policy create root amb-log-sync-write-only /tmp/policy.json
mc admin user add root "$(openssl rand -hex 10)" "$(openssl rand -hex 20)"
EOF
```

`mc admin user add` prints the access key and secret key it just generated --
capture them from that output rather than choosing your own, so nothing types a
credential by hand into a shell history. Then attach the policy to that key:

```bash
docker run --rm \
  --network "$MINIO_NETWORK" \
  -e MC_HOST_root="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@homelab-minio:9000" \
  minio/mc:latest mc admin policy attach root amb-log-sync-write-only \
  --user <the-access-key-just-created>
```

(`mc`'s policy-attach subcommand has changed name across versions -- `mc admin
policy attach ALIAS POLICY --user=KEY` on current releases, `mc admin policy set
ALIAS POLICY user=KEY` on older ones. Check `mc admin policy --help` inside the
container if the first form is rejected.)

## Installing on amblevilleserv

```bash
# 2. Put the two credentials from step 1 in a file only this user can read.
#    AMB_LOG_SYNC_CREDENTIALS_FILE overrides the default path, which is what
#    the installed copy uses -- see "How it is actually installed" below.
umask 077
mkdir -p ~/amb-log-sync
cat > ~/amb-log-sync/credentials.env <<'EOF'
AMB_LOG_SYNC_ACCESS_KEY=<the-access-key>
AMB_LOG_SYNC_SECRET_KEY=<the-secret-key>
EOF
chmod 600 ~/amb-log-sync/credentials.env

# 3. Get this directory onto amblevilleserv (scp, a checkout, whatever is already
#    already used there) and confirm one run works:
AMB_LOG_SYNC_CREDENTIALS_FILE=~/amb-log-sync/credentials.env \
  bash ~/amb-log-sync/sync-amb-logs.sh

# 4. Cron, hourly, a few minutes past the hour so it never races NPM's own
#    logrotate (which runs at :00 on this host -- see IMPLEMENTATION_PLAN.md):
crontab -e
```

Cron line:

```
7 * * * * AMB_LOG_SYNC_CREDENTIALS_FILE=/home/francois/amb-log-sync/credentials.env /home/francois/amb-log-sync/sync-amb-logs.sh >>/home/francois/amb-log-sync/sync.log 2>&1
```

## How it is actually installed (2026-09-15)

Two things about amblevilleserv that the procedure above has to bend around, both
found while provisioning it for real rather than on paper.

**There is no sudo on that host for this operator.** So nothing lives under
`/etc` or `/var/log`: the credentials file, both scripts and the cron log all sit
in `~/amb-log-sync/` (mode 600 on the credentials, which is what
`sync-amb-logs.sh` checks for -- the script refuses a file that is not 600 or
400, and refuses a missing one, rather than falling through to an anonymous
`mc`). The cron entry is a user crontab, not a system one.

**That host's Docker cannot pull `minio/mc` from Docker Hub** -- it answers
`pull access denied for minio/mc` for that namespace, while `library/` images
pull normally. The image was moved across from fraserv instead, which already
had it:

```bash
ssh fraserv 'docker save minio/mc:latest' | ssh amblevilleserv 'docker load'
```

`docker run` only pulls when the image is absent locally, so the hourly job is
fine as long as that image stays. **If it is ever pruned on amblevilleserv the
cron starts failing with a pull error, and the fix is the line above, not a
retry.** Worth knowing before reading `sync.log` and concluding the key broke.

## Verifying

```bash
# From fraserv, or anywhere with the datalake key (not the write-only one --
# that key cannot list, so this is a check for the operator, not the script):
docker exec datalake-daemon python -c "
from homelab_datalake.resources import LakeStorage
lake = LakeStorage.from_env()
keys = lake.list_keys('inbox/npm/amb')
print(len(keys), 'objects')
for k in keys[:10]: print(' ', k)
"
```

A healthy inbox holds the live `*_access.log` plus whatever rotated `.gz`
generations amblevilleserv's own logrotate has not aged out yet -- the same shape
`discover_logs` expects from a mounted directory. `npm_access_raw` treats an empty
inbox as a broken sync and refuses to write an empty partition (see
`AMB_INBOX_PREFIX` and its `FileNotFoundError` in `assets.npm_access`), so a
stalled cron job fails loudly there rather than silently landing zero rows.
