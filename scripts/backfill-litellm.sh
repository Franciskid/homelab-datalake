#!/usr/bin/env bash
# Rebuilds a range of LiteLLM spend partitions, one Dagster run each.
#     bash scripts/backfill-litellm.sh 2026-06-16 2026-08-28
# Runs ON primary.
#
# Same reason as the NPM backfill: the caller's address is looked up for geography and
# discarded in the same step, so geo_country_code and geo_as_org on fct_llm_call cannot
# be filled by rebuilding the marts. The source rows have to be read again.
set -euo pipefail
START="${1:?usage: backfill-litellm.sh START END}"
END="${2:?usage: backfill-litellm.sh START END}"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
day="$START"; ok=0; failed=()
while [[ "$day" < "$END" || "$day" == "$END" ]]; do
  if docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon \
       dagster asset materialize --select litellm_spend_raw \
       --partition "$day" -m homelab_datalake.definitions >/dev/null 2>&1; then
    ok=$((ok + 1))
  else
    failed+=("$day")
  fi
  day=$(date -u -d "$day + 1 day" +%Y-%m-%d)
done
echo "== $ok partitions rebuilt"
if [[ ${#failed[@]} -gt 0 ]]; then echo "== ${#failed[@]} failed: ${failed[*]}"; exit 1; fi
