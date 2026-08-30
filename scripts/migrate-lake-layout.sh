#!/usr/bin/env bash
# Move the MinIO bucket from raw/<table>/ to <layer>/<domain>/<source>/<dataset>/.
#
# Runs inside datalake-daemon, which already holds the bucket credentials. Reports
# the plan and stops; pass --apply to carry it out. Safe to re-run: an object already
# at its new key is counted, not copied again.
#
#   bash scripts/migrate-lake-layout.sh            # show the plan
#   bash scripts/migrate-lake-layout.sh --apply    # do it
set -euo pipefail

HOST="${DATALAKE_SSH_HOST:-ssh.example.test}"
MODE="plan"
[[ "${1:-}" == "--apply" ]] && MODE="apply"

ssh "$HOST" "docker exec -i -e MIGRATE_MODE=$MODE datalake-daemon python -" <<'PYEOF'
import os
from homelab_datalake import migrate_layout
from homelab_datalake.resources import LakeStorage

lake = LakeStorage.from_env()
print(f"lake: {lake.describe()}")

objects = migrate_layout.inventory(lake)
plan = migrate_layout.plan(objects)

print(f"{len(objects)} objects total")
print(f"{len(plan.moves)} to move ({plan.bytes_to_move/1e6:.2f} MB)")
print(f"{plan.already_moved} already in the new layout")
if plan.unknown:
    print(f"{len(plan.unknown)} under no known prefix, left alone:")
    for key in plan.unknown[:20]:
        print(f"    {key}")

seen = set()
for move in plan.moves:
    pair = (move.old_key.rsplit("/", 1)[0], move.new_key.rsplit("/", 1)[0])
    root = (pair[0].split("/")[1], pair[1])
    if root[0] not in seen:
        seen.add(root[0])
        print(f"  {root[0]:24s} -> {'/'.join(pair[1].split('/')[:4])}")

if os.environ["MIGRATE_MODE"] != "apply":
    print("\nplan only. Re-run with --apply to move them.")
    raise SystemExit(0)

moved = migrate_layout.apply(lake, plan)
print(f"\nmoved {moved} objects")

after = migrate_layout.inventory(lake)
if len(after) != len(objects):
    raise SystemExit(f"FAIL {len(objects)} objects before, {len(after)} after")
leftover = migrate_layout.plan(after)
if leftover.moves:
    raise SystemExit(f"FAIL {len(leftover.moves)} objects still under a legacy prefix")
print(f"OK    {len(after)} objects, all under the new layout")
PYEOF
