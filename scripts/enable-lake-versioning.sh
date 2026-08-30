#!/usr/bin/env bash
# Set up both lake buckets with the MinIO root credentials. Run once.
#
# Three things, all of which need admin rights the datalake key deliberately does not
# have:
#
#   1. Create the backup bucket the nightly mirror writes to. Measured: the datalake
#      key gets AccessDenied on CreateBucket, which is correct -- a key used every
#      night should not be able to create or reconfigure buckets.
#   2. Turn on versioning for both buckets. MinIO then keeps the previous object after
#      every overwrite and every delete, so recovering one is immediate and needs no
#      backup at all. That is the first of the three layers protecting the lake; the
#      others are the nightly mirror (lake_backup) and scripts/restore-lake.sh.
#   3. Add the lifecycle rule that stops those old versions accumulating for ever.
#
#   bash scripts/enable-lake-versioning.sh            # show what it would do
#   bash scripts/enable-lake-versioning.sh --apply    # do it
#
# Nothing here is destructive: it creates a bucket, turns versioning on, and adds a
# rule that expires *non-current* versions only. The script refuses outright if any
# lifecycle rule would expire a current one.
#
# The datalake S3 key cannot do this. It is an ordinary access key -- measured:
# get_bucket_versioning returns AccessDenied -- so this runs once with the MinIO root
# credentials. They are read from your environment or prompted for, never written to
# disk, never put on a command line where `ps` would show them, and never stored in
# .env. Export them for this one run:
#
#   export MINIO_ROOT_USER=... MINIO_ROOT_PASSWORD=...
#
# The lifecycle rule is the other half and is not optional. Versioning without an
# expiry means every overwritten partition is kept for ever, and the host this runs on
# is [rate withheld] full. Thirty days is long enough to notice a mistake and short enough that
# the bucket stops growing.
set -euo pipefail

HOST="${DATALAKE_SSH_HOST:-ssh.example.test}"
MODE="plan"
[[ "${1:-}" == "--apply" ]] && MODE="apply"

if [[ -z "${MINIO_ROOT_USER:-}" ]]; then
  read -rp "MinIO root user: " MINIO_ROOT_USER
fi
if [[ -z "${MINIO_ROOT_PASSWORD:-}" ]]; then
  read -rsp "MinIO root password: " MINIO_ROOT_PASSWORD
  echo
fi

# Base64 through the program text on stdin, rather than an environment variable or an
# argument. `docker exec -e` would put the secret in the container's environment,
# where anything running in it can read it for the life of the exec; an argument would
# put it in `ps` on both machines. stdin is seen by this process, ssh, and python.
CREDS=$(printf '%s\n%s' "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" | base64 | tr -d '\n')

ssh "$HOST" "docker exec -i -e VERSIONING_MODE=$MODE datalake-daemon python -" <<PYEOF
import base64, json, os

import boto3

from homelab_datalake.assets import backup
from homelab_datalake.resources import LakeStorage

user, password = base64.b64decode("$CREDS").decode().split("\n")
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
# Expiration key is the whole safety property of this rule: with one, this would be a
# scheduled deletion of the lake rather than a bound on its history.
RULE = {
    "ID": "expire-noncurrent-versions",
    "Status": "Enabled",
    "Filter": {"Prefix": ""},
    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
}

plans = {}
for name in BUCKETS:
    try:
        existing = root.get_bucket_lifecycle_configuration(Bucket=name)["Rules"]
    except Exception:
        existing = []
    # Existing rules are kept. put_bucket_lifecycle_configuration replaces rather
    # than merges, so writing only ours would silently drop one somebody added for
    # another reason.
    merged = [r for r in existing if r.get("ID") != RULE["ID"]] + [RULE]
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
    print(f"    lifecycle       {name}: {json.dumps(RULE)}")

if os.environ["VERSIONING_MODE"] != "apply":
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
    if not any(r.get("ID") == RULE["ID"] for r in rules):
        raise SystemExit(f"FAIL the lifecycle rule is not on {name} after the change")
    print(f"OK    {name}: versioning Enabled, {len(rules)} lifecycle rule(s)")

ordinary = lake._client()
for name in BUCKETS:
    listed = ordinary.list_objects_v2(Bucket=name, MaxKeys=1)
    print(f"OK    the datalake key still lists {name} ({listed.get('KeyCount', 0)} key sampled)")

print()
print("Now materialise lake_backup, then run scripts/restore-lake.sh as a drill.")
PYEOF
