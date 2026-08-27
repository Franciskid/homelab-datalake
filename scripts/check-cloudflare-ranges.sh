#!/usr/bin/env bash
# Does NPM's generated trust list still cover every range Cloudflare publishes?
#
# NPM maintains conf.d/include/ip_ranges.conf itself and regenerates it on container
# start, so this is a check rather than a deploy. A range Cloudflare adds after NPM's
# last restart is missed until the container restarts -- which fails safe: those
# requests keep the edge address instead of trusting an unverified header.
#
# Usage: scripts/check-cloudflare-ranges.sh [ssh-host]   (default: ssh.example.test)
set -uo pipefail
HOST="${1:-ssh.example.test}"

published=$(mktemp); trap 'rm -f "$published"' EXIT
{ curl -sS --max-time 20 https://www.cloudflare.com/ips-v4
  echo
  curl -sS --max-time 20 https://www.cloudflare.com/ips-v6
} | tr -d '\r' | grep . | sort -u > "$published"

count=$(wc -l < "$published")
if [ "$count" -lt 15 ]; then
  echo "FAIL: fetched only $count ranges from Cloudflare -- refusing to compare"; exit 1
fi

trusted=$(ssh "$HOST" 'docker exec npm-npm-1 cat /etc/nginx/conf.d/include/ip_ranges.conf')

missing=0
while read -r range; do
  case "$trusted" in
    *"set_real_ip_from $range;"*) ;;
    *) echo "MISSING from NPM's trust list: $range"; missing=$((missing+1)) ;;
  esac
done < "$published"

echo "$HOST: $count published ranges, $missing missing"
[ "$missing" -eq 0 ] || { echo "Restart npm-npm-1 to regenerate ip_ranges.conf."; exit 1; }
