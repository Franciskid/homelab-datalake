#!/usr/bin/env bash
#
# Sends this working copy to primary, builds the images, and rebuilds the marts.
#
#   bash scripts/deploy.sh            # sync, build, run analytics_daily, test
#   bash scripts/deploy.sh --no-run   # sync and build only
#
# Run it from a clone on your own machine. It is the deploy step this repository has
# been missing: the server holds an rsync copy and not a git checkout, and the code is
# baked into the image rather than mounted. So a change to a .py or a .sql file needs
# two steps -- copy it, then build -- and doing only the first is silent. That is what
# happened on 29 Aug 2026: _marts.yml reached the host, the container kept its own
# copy, and the same test failed twice with the same message.

set -euo pipefail

FRA=ssh.example.test
DIR=/srv/projects/homelab-datalake
RUN_ANALYTICS=1
[[ "${1:-}" == "--no-run" ]] && RUN_ANALYTICS=0

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
here() { cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd; }
cd "$(here)"

say "1/4  Sync"
# --delete on the source trees only. It removes a file on the server that no longer
# exists here, which is how a deleted model or test actually leaves the deployment.
# .env, the warehouse and the Dagster home are never touched.
for tree in src dbt/models dbt/tests tests; do
  rsync -a --delete --no-perms --no-owner --no-group \
        --exclude='__pycache__' --exclude='*.pyc' \
        "$tree/" "$FRA:$DIR/$tree/"
  echo "  OK    $tree"
done
rsync -a --no-perms --no-owner --no-group docker-compose.yml Dockerfile pyproject.toml "$FRA:$DIR/"
echo "  OK    docker-compose.yml, Dockerfile, pyproject.toml"
# Operator scripts. Some of them have to run ON the server -- the Keycloak ones
# exec into a container that only exists there -- so a script that never leaves
# this laptop is a script nobody can run. This was the gap: four scripts existed
# here and none of them had ever reached the host.
#
# No --delete on this one, unlike the source trees. scripts/ is somewhere an
# operator legitimately drops a one-off, and a deploy should not remove it.
rsync -a --no-perms --no-owner --no-group --exclude='__pycache__' \
      scripts/ "$FRA:$DIR/scripts/"
echo "  OK    scripts"

say "2/4  Build and start"
ssh "$FRA" "cd $DIR && docker compose up -d --build"

say "3/4  Confirm the image carries this working copy"
# Compares a checksum of the deployed tree against the same checksum computed here.
# A mismatch means the build did not pick the change up, which is the failure this
# script exists to make impossible to miss.
local_sum=$(find src dbt/models dbt/tests -type f \( -name '*.py' -o -name '*.sql' -o -name '*.yml' \) \
            | sort | xargs shasum | shasum | cut -d' ' -f1)
image_sum=$(ssh "$FRA" "docker exec datalake-daemon sh -c \"cd /app && find src dbt/models dbt/tests -type f \\( -name '*.py' -o -name '*.sql' -o -name '*.yml' \\) | sort | xargs shasum | shasum | cut -d' ' -f1\"")
if [[ "$local_sum" == "$image_sum" ]]; then
  echo "  OK    the image matches this working copy ($local_sum)"
else
  echo "  FAIL  the image does not match this working copy"
  echo "        here:  $local_sum"
  echo "        image: $image_sum"
  exit 1
fi

if [[ "$RUN_ANALYTICS" == "1" ]]; then
  say "4/4  Rebuild the marts"
  ssh "$FRA" "docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon \
    dagster job execute -j analytics_daily -m homelab_datalake.definitions" 2>&1 \
    | grep -E "Done\. PASS|FAIL [0-9]|RUN_SUCCESS|RUN_FAILURE|published" || true
else
  say "4/4  Skipped (--no-run)"
fi
