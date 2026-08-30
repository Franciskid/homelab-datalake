#!/usr/bin/env bash
# Put objects back into the lake from the nightly mirror.
#
# A backup nobody has restored is not a backup. This is the script that makes the
# third layer real, and running it is a step of the workbench plan rather than
# something to reach for the first time during an incident.
#
#   bash scripts/restore-lake.sh                                   # everything, plan
#   bash scripts/restore-lake.sh --dataset npm_access              # one dataset
#   bash scripts/restore-lake.sh --dataset npm_access --day 2026-08-01
#   bash scripts/restore-lake.sh --apply                           # carry it out
#
# It reports and stops. Nothing is written without --apply, and after --apply it
# re-lists both sides and compares object counts and sizes -- a restore that reports
# success from the return value of the thing that did the copying has verified
# nothing.
#
# Only raw/ is in the mirror. curated/ and marts/ are rebuilt from raw/ by
# analytics_daily, so the recovery for those is to re-run that job -- which is also
# what has to happen after any restore, because the warehouse still holds whatever it
# held before.
set -euo pipefail

HOST="${DATALAKE_SSH_HOST:-ssh.example.test}"
MODE="plan"
DATASET=""
DAY=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)   MODE="apply"; shift ;;
    --dataset) DATASET="${2:?--dataset needs a table name}"; shift 2 ;;
    --day)     DAY="${2:?--day needs a date as YYYY-MM-DD}"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

ssh "$HOST" "docker exec -i \
  -e RESTORE_MODE=$MODE -e RESTORE_DATASET='$DATASET' -e RESTORE_DAY='$DAY' \
  datalake-daemon python -" <<'PYEOF'
import os

from homelab_datalake.assets import backup
from homelab_datalake.resources import LakeStorage

lake = LakeStorage.from_env()
vault = backup.backup_storage(lake)
dataset = os.environ.get("RESTORE_DATASET") or None
day = os.environ.get("RESTORE_DAY") or None

scope = backup.restore_scope(dataset)
# The day is matched as a path segment rather than appended to the prefix. npm
# partitions on host *and* day, and the host comes first in the key, so
# `<prefix>/dt=<day>` matches nothing there -- and matching nothing is reported as
# "0 objects to restore", which during an incident reads as an empty backup.
segment = backup.day_segment(dataset, day) if (dataset and day) else None
print(f"lake:   {lake.describe()}")
print(f"backup: {vault.describe()}")
print(f"scope:  {scope}" + (f"  (day {segment})" if segment else ""))
print()

plan = backup.plan_restore(lake, vault, scope, segment)

print(f"{len(plan.restore):5d} to restore ({plan.bytes_to_restore/1e6:.2f} MB)")
print(f"{len(plan.identical):5d} already identical in the lake")
print(f"{len(plan.differing):5d} present in the lake and DIFFERENT -- these get overwritten")
print(f"{len(plan.missing_from_backup):5d} in the lake and not in the backup -- left alone")

for obj in plan.restore[:20]:
    marker = "overwrite" if obj.key in plan.differing else "restore  "
    print(f"    {marker}  {obj.key}  ({obj.size} bytes)")
if len(plan.restore) > 20:
    print(f"    ... and {len(plan.restore) - 20} more")

if not plan.restore:
    print("\nnothing to restore. The lake already holds everything the backup has.")
    raise SystemExit(0)

if os.environ["RESTORE_MODE"] != "apply":
    print("\nplan only. Re-run with --apply to carry it out.")
    raise SystemExit(0)

print()
restored = backup.apply_restore(lake, plan, vault)
print(f"restored {restored} objects")

problems = backup.verify_restore(lake, plan, vault, scope, segment)
if problems:
    print(f"\nFAIL {len(problems)} object(s) still disagree:")
    for line in problems[:20]:
        print(f"    {line}")
    raise SystemExit(1)

live = backup._within(backup.inventory(lake, scope), segment)
held = backup._within(backup.inventory(vault, scope), segment)
print(f"OK    {len(live)} objects in the lake, {len(held)} in the backup, sizes match")
print()
print("The warehouse still holds what it held before this restore. Run analytics_daily")
print("to rebuild it, or every mart is still derived from the data that was missing.")
PYEOF
