#!/usr/bin/env bash
# Turn on versioning for the datalake bucket, and bound what versioning keeps.
#
# MinIO then keeps the previous object after every overwrite and every delete, so
# recovering one object is immediate and needs no backup at all. That is the first of
# the three layers protecting the lake; the other two are the nightly mirror
# (lake_backup) and scripts/restore-lake.sh.
#
#   bash scripts/enable-lake-versioning.sh            # show what it would do
#   bash scripts/enable-lake-versioning.sh --apply    # do it
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

print(f"bucket: {lake.bucket} at {lake.endpoint}")

status = root.get_bucket_versioning(Bucket=lake.bucket).get("Status", "Disabled")
print(f"versioning now: {status}")

# 30 days of non-current versions, and no expiry of current ones. The absence of an
# Expiration key is the whole safety property of this rule: with one, this would be a
# scheduled deletion of the lake rather than a bound on its history.
RULE = {
    "ID": "expire-noncurrent-versions",
    "Status": "Enabled",
    "Filter": {"Prefix": ""},
    "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
}

try:
    existing = root.get_bucket_lifecycle_configuration(Bucket=lake.bucket)["Rules"]
except Exception:
    existing = []
print(f"lifecycle rules now: {len(existing)}")
for rule in existing:
    print(f"    {rule.get('ID')}: {json.dumps({k: v for k, v in rule.items() if k != 'ID'})}")

# Existing rules are kept. Replacing the configuration wholesale would silently drop
# a rule somebody added for another reason -- put_bucket_lifecycle_configuration
# replaces, it does not merge.
merged = [r for r in existing if r.get("ID") != RULE["ID"]] + [RULE]
for rule in merged:
    if "Expiration" in rule:
        raise SystemExit(
            f"FAIL rule {rule.get('ID')!r} expires *current* versions. That deletes "
            "the lake on a timer. Nothing was changed; remove it by hand first."
        )

print()
print("would set:")
print("    versioning: Enabled")
print(f"    lifecycle:  {json.dumps(RULE)}")

if os.environ["VERSIONING_MODE"] != "apply":
    print("\nplan only. Re-run with --apply.")
    raise SystemExit(0)

root.put_bucket_versioning(Bucket=lake.bucket, VersioningConfiguration={"Status": "Enabled"})
root.put_bucket_lifecycle_configuration(
    Bucket=lake.bucket, LifecycleConfiguration={"Rules": merged}
)

# Read both back with the root key, then prove the ordinary key still works. A
# versioning change that broke the pipeline's own access would present as every
# nightly job failing, hours later, for no visible reason.
after = root.get_bucket_versioning(Bucket=lake.bucket).get("Status")
rules = root.get_bucket_lifecycle_configuration(Bucket=lake.bucket)["Rules"]
if after != "Enabled":
    raise SystemExit(f"FAIL versioning is {after!r} after the change")
if not any(r.get("ID") == RULE["ID"] for r in rules):
    raise SystemExit("FAIL the lifecycle rule is not there after the change")

listed = lake._client().list_objects_v2(Bucket=lake.bucket, MaxKeys=1)
print(f"\nOK    versioning Enabled, {len(rules)} lifecycle rule(s)")
print(f"OK    the datalake key still lists the bucket ({listed.get('KeyCount', 0)} key sampled)")
PYEOF
