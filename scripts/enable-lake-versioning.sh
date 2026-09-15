#!/usr/bin/env bash
# Set up both lake buckets with the MinIO root credentials. Run once, on primary.
#
#   bash scripts/enable-lake-versioning.sh            # show what it would do
#   bash scripts/enable-lake-versioning.sh --apply    # do it
#
# Three things, all needing admin rights the datalake key deliberately does not have:
#
#   1. Create the backup bucket the nightly mirror writes to. Measured: the datalake
#      key gets AccessDenied on CreateBucket, which is correct -- a key used every
#      night should not be able to create or reconfigure buckets.
#   2. Turn on versioning for both buckets. MinIO then keeps the previous object after
#      every overwrite and every delete, so recovering one is immediate and needs no
#      backup at all. That is the first of the three layers protecting the lake; the
#      others are the nightly mirror (lake_backup) and scripts/restore-lake.sh.
#   3. Add the lifecycle rules that stop those old versions accumulating for ever: 30
#      days for everything, 1 day under raw/infrastructure/prometheus/, whose minute
#      parts are deleted by compaction within a day of being written.
#
# NOBODY NEEDS TO KNOW THE ROOT PASSWORD
#
# It is already on this host, in the MinIO container's own environment, which is how
# MinIO was configured in the first place. This reads it from there and pipes it
# straight into the job on stdin. It is never an argument (so never in `ps`), never
# put in another container's environment, never echoed, and never written down.
#
# That is not only convenience. A credential a human has to read and retype is a
# credential that ends up in a scrollback, a clipboard and a shell history -- and
# rotating the MinIO root password to work around having forgotten it would have
# meant re-issuing every key derived from it.
#
# Override with MINIO_ROOT_USER / MINIO_ROOT_PASSWORD in the environment, or let it
# prompt, if MinIO runs somewhere this cannot see it.
#
# Nothing here is destructive: it creates a bucket, turns versioning on, and adds a
# rule that expires *non-current* versions only. It refuses outright if any lifecycle
# rule on either bucket would expire a current one.
set -euo pipefail

MODE="plan"
[[ "${1:-}" == "--apply" ]] && MODE="apply"

MINIO_CONTAINER="${MINIO_CONTAINER:-homelab-minio}"
DAEMON="${DATALAKE_DAEMON_CONTAINER:-datalake-daemon}"

# --- the root credentials -------------------------------------------------------
if [[ -z "${MINIO_ROOT_USER:-}" || -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
  if docker inspect "$MINIO_CONTAINER" >/dev/null 2>&1; then
    echo "Reading the root credentials from the $MINIO_CONTAINER container."
    creds=$(docker inspect "$MINIO_CONTAINER" \
      --format '{{range .Config.Env}}{{println .}}{{end}}')
    MINIO_ROOT_USER="${MINIO_ROOT_USER:-$(grep -m1 '^MINIO_ROOT_USER=' <<<"$creds" | cut -d= -f2-)}"
    MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-$(grep -m1 '^MINIO_ROOT_PASSWORD=' <<<"$creds" | cut -d= -f2-)}"
    unset creds
  fi
fi

if [[ -z "${MINIO_ROOT_USER:-}" ]]; then
  read -rp "MinIO root user: " MINIO_ROOT_USER
fi
if [[ -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
  read -rsp "MinIO root password: " MINIO_ROOT_PASSWORD
  echo
fi

if [[ -z "$MINIO_ROOT_USER" || -z "$MINIO_ROOT_PASSWORD" ]]; then
  echo "FAIL  no root credentials. Nothing was changed." >&2
  exit 1
fi
echo "Using root user: $MINIO_ROOT_USER"

# --- the job ---------------------------------------------------------------------
#
# Written into the container as a file rather than piped as a program, so the
# credentials can have stdin to themselves -- and so a traceback names real line
# numbers instead of "<stdin>".
docker exec -i "$DAEMON" sh -c 'cat > /tmp/lake_versioning.py' <<'PYEOF'
"""Create the backup bucket, version both, and bound what versioning keeps."""
import json
import os
import sys

import boto3

from homelab_datalake.assets import backup
from homelab_datalake.resources import LakeStorage

# The credentials arrive on stdin and nowhere else: not argv, not the environment.
user = sys.stdin.readline().rstrip("\n")
password = sys.stdin.readline().rstrip("\n")
if not user or not password:
    raise SystemExit("no credentials arrived on stdin")

lake = LakeStorage.from_env()
if not lake.uses_s3:
    raise SystemExit("this lake is a local directory; versioning is a MinIO feature")

root = boto3.client(
    "s3",
    endpoint_url=lake.endpoint,
    aws_access_key_id=user,
    aws_secret_access_key=password,
    region_name=lake.region,
)

# Both buckets. The mirror is versioned too: it is the copy that has to survive a
# mistake made in the copy.
BUCKETS = [lake.bucket, backup.backup_storage(lake).bucket]
print(f"endpoint: {lake.endpoint}")
print(f"buckets:  {', '.join(BUCKETS)}")
print()

missing = []
for name in BUCKETS:
    try:
        root.head_bucket(Bucket=name)
        status = root.get_bucket_versioning(Bucket=name).get("Status", "Disabled")
        print(f"  {name}: exists, versioning {status}")
    except Exception:
        missing.append(name)
        print(f"  {name}: does not exist yet")

# 30 days of non-current versions, and no expiry of current ones. The absence of an
# Expiration key is the whole safety property of this rule: with one, it would be a
# scheduled deletion of the lake rather than a bound on its history.
RULE = {
    "ID": "expire-noncurrent-versions",
    "Status": "Enabled",
    "Filter": {"Prefix": ""},
    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
}
# The Prometheus stream writes an object a minute and compaction deletes them hourly and
# daily. Under the 30-day rule every deleted part would stay as a non-current version for
# a month -- [volume withheld] of objects that were only ever a staging step for a merged file that
# is itself versioned. One day is enough to recover from a compaction that went wrong.
STREAM_RULE = {
    "ID": "expire-noncurrent-prometheus-parts",
    "Status": "Enabled",
    "Filter": {"Prefix": "raw/infrastructure/prometheus/"},
    "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
}
RULES = [RULE, STREAM_RULE]

plans = {}
for name in BUCKETS:
    try:
        existing = root.get_bucket_lifecycle_configuration(Bucket=name)["Rules"]
    except Exception:
        existing = []
    # Existing rules are kept. put_bucket_lifecycle_configuration replaces rather
    # than merges, so writing only ours would silently drop one somebody added for
    # another reason.
    merged = [r for r in existing if r.get("ID") not in {r["ID"] for r in RULES}] + RULES
    for rule in merged:
        if "Expiration" in rule:
            raise SystemExit(
                f"FAIL rule {rule.get('ID')!r} on {name} expires *current* versions. "
                "That deletes the bucket on a timer. Nothing was changed; remove it "
                "by hand first."
            )
    plans[name] = merged

print()
print("would do:")
for name in BUCKETS:
    if name in missing:
        print(f"    create bucket   {name}")
    print(f"    versioning      {name}: Enabled")
    for rule in RULES:
        print(f"    lifecycle       {name}: {json.dumps(rule)}")

if os.environ.get("VERSIONING_MODE") != "apply":
    print("\nplan only. Re-run with --apply.")
    raise SystemExit(0)

print()
for name in BUCKETS:
    if name in missing:
        root.create_bucket(Bucket=name)
        print(f"created {name}")
    root.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
    root.put_bucket_lifecycle_configuration(
        Bucket=name, LifecycleConfiguration={"Rules": plans[name]}
    )

# Read it all back with the root key, then prove the ordinary key still works.
# Getting this wrong would present as every nightly job failing hours later for no
# visible reason, so it is checked here rather than discovered then.
print()
for name in BUCKETS:
    status = root.get_bucket_versioning(Bucket=name).get("Status")
    rules = root.get_bucket_lifecycle_configuration(Bucket=name)["Rules"]
    if status != "Enabled":
        raise SystemExit(f"FAIL versioning on {name} is {status!r} after the change")
    for wanted in RULES:
        if not any(r.get("ID") == wanted["ID"] for r in rules):
            raise SystemExit(f"FAIL lifecycle rule {wanted['ID']} is not on {name} after the change")
    print(f"OK    {name}: versioning Enabled, {len(rules)} lifecycle rule(s)")

ordinary = lake._client()
for name in BUCKETS:
    listed = ordinary.list_objects_v2(Bucket=name, MaxKeys=1)
    print(f"OK    the datalake key still lists {name} ({listed.get('KeyCount', 0)} key sampled)")

print()
print("Now materialise lake_backup, then run scripts/restore-lake.sh as a drill.")
PYEOF

printf '%s\n%s\n' "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" \
  | docker exec -i -e VERSIONING_MODE="$MODE" "$DAEMON" python /tmp/lake_versioning.py

docker exec "$DAEMON" rm -f /tmp/lake_versioning.py
