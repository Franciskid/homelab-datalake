#!/usr/bin/env bash
#
# Rebuilds a range of NPM partitions, one Dagster run each.
#
#     bash scripts/backfill-npm.sh 2026-07-27 2026-08-29 [host]
#
# Runs ON primary.
#
# One run per partition rather than one run over a range, for the reason the asset is
# partitioned by (date, host) in the first place: a day that fails must not mark the
# others failed, and a rerun of one day must not touch another. It is slower -- each run
# is its own process -- and that is the trade the design already accepted.
#
# The whole range is rebuilt and not only the absent days. Geography is looked up while
# the address is still in hand and discarded immediately after, so geo_country_code
# cannot be filled in later by rebuilding the marts. The only way an old day gains
# geography is to parse its log lines again.

set -euo pipefail

START="${1:?usage: backfill-npm.sh START END [host]}"
END="${2:?usage: backfill-npm.sh START END [host]}"
HOST="${3:-primary}"

cd "$(dirname "${BASH_SOURCE[0]}")/.."

day="$START"
ok=0
failed=()
while [[ "$day" < "$END" || "$day" == "$END" ]]; do
  printf '%s|%s ... ' "$day" "$HOST"
  if out=$(docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon \
            dagster asset materialize --select npm_access_raw \
            --partition "$day|$HOST" -m homelab_datalake.definitions 2>&1); then
    # The asset logs its own counts; surface just that line.
    echo "$out" | grep -oE "dt=[0-9-]+ host=[a-z]+ [0-9,]+ rows.*" | head -1 || echo "done"
    ok=$((ok + 1))
  else
    echo "FAILED"
    echo "$out" | tail -3
    failed+=("$day")
  fi
  day=$(date -u -d "$day + 1 day" +%Y-%m-%d)
done

echo
echo "== $ok partitions rebuilt"
if [[ ${#failed[@]} -gt 0 ]]; then
  echo "== ${#failed[@]} failed: ${failed[*]}"
  exit 1
fi
