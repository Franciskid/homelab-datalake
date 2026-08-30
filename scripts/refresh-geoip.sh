#!/usr/bin/env bash
#
# Downloads the MaxMind GeoLite2 databases into the datalake's geoip volume.
#
# Runs ON primary, next to the volume:
#     bash scripts/refresh-geoip.sh
#
# MAXMIND_LICENSE_KEY must be in .env beside it. The key is a credential and never
# belongs in the repository.
#
# WHY A VOLUME AND NOT A BIND MOUNT
# The containers mount `geoip:/geoip:ro`. Read-only is the point: the pipeline reads
# geography, it does not maintain it. So this script writes through its own short-lived
# container that has the volume read-write, and the pipeline never holds a handle that
# could damage the database it depends on.
#
# WHY ATOMIC
# Each file is written under a temporary name and then moved. A move within one
# filesystem is atomic, so a reader either sees last month's complete database or this
# month's complete database. It never sees half a download. GeoIP lookups happen inside
# the ingest loop, so a truncated file would not fail the run -- it would quietly return
# nulls for part of a day, which is the failure mode that is hardest to notice.
#
# MaxMind rebuilds these twice a week. Run it monthly; there is a cron line at the end.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VOLUME=homelab-datalake_geoip
EDITIONS=(GeoLite2-City GeoLite2-ASN)

# shellcheck disable=SC1091
set -a; source "$DIR/.env"; set +a

: "${MAXMIND_LICENSE_KEY:?MAXMIND_LICENSE_KEY is not set in .env}"

echo "== Downloading into the volume $VOLUME"

for edition in "${EDITIONS[@]}"; do
  # One container per edition. alpine has tar and wget; nothing is installed on the host.
  #
  # The key is passed as an environment variable and not on the command line, so it does
  # not appear in `docker ps`, in the host's process list, or in this script's own trace
  # output under `set -x`.
  docker run --rm \
    -e MAXMIND_LICENSE_KEY \
    -e EDITION="$edition" \
    -v "$VOLUME:/geoip" \
    alpine:3.20 sh -euc '
      apk add --no-cache curl tar >/dev/null 2>&1
      url="https://download.maxmind.com/app/geoip_download?edition_id=${EDITION}&license_key=${MAXMIND_LICENSE_KEY}&suffix=tar.gz"
      tmp=$(mktemp -d)
      # --fail turns an HTTP error into a non-zero exit. Without it curl writes the
      # error page to the archive and tar fails later with a confusing message.
      curl -sS --fail -L "$url" -o "$tmp/db.tar.gz"
      tar -xzf "$tmp/db.tar.gz" -C "$tmp"
      src=$(find "$tmp" -name "*.mmdb" | head -1)
      test -n "$src" || { echo "no .mmdb inside the archive for ${EDITION}"; exit 1; }
      # Write beside the target, then move. Both are on the volume, so the move is atomic.
      cp "$src" "/geoip/.${EDITION}.mmdb.partial"
      mv "/geoip/.${EDITION}.mmdb.partial" "/geoip/${EDITION}.mmdb"
      rm -rf "$tmp"
      echo "  OK    ${EDITION}.mmdb  $(stat -c%s "/geoip/${EDITION}.mmdb") bytes"
    '
done

echo
echo "== Contents"
docker run --rm -v "$VOLUME:/geoip" alpine:3.20 ls -la /geoip

cat <<'NOTE'

Set these in .env if they are not there yet, then rebuild:

    GEOIP_CITY_DB=/geoip/GeoLite2-City.mmdb
    GEOIP_ASN_DB=/geoip/GeoLite2-ASN.mmdb

To keep it current, MaxMind rebuilds twice a week:

    5 4 1 * *  cd /srv/projects/homelab-datalake && bash scripts/refresh-geoip.sh >> /tmp/geoip-refresh.log 2>&1
NOTE
