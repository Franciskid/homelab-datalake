#!/usr/bin/env bash
# Cloudflare share of logged client addresses, broken down by hour.
#
# A plain total is misleading here: the log window reaches back before the cutover,
# so it averages old rows with new. Per hour you can watch the change land.
#
# Expect ~[rate withheld] for recent hours on primary after the server_proxy.conf change, and
# ~[rate withheld] throughout on secondary, which never sat behind the proxy. A recent hour
# climbing back up means the header override was lost -- check that
# /srv/npm/data/nginx/custom/server_proxy.conf still exists.
#
# Usage: scripts/verify-real-ip.sh [ssh-host] [lines-per-log] [hours-shown]
set -uo pipefail
HOST="${1:-ssh.example.test}"; LINES="${2:-2000}"; HOURS="${3:-8}"

ssh "$HOST" "docker exec npm-npm-1 sh -c 'tail -qn $LINES /data/logs/proxy-host-*_access.log'" \
| python3 -c '
import re, sys, collections

CF = re.compile(r"^(172\.(6[4-9]|7[01])\.|104\.(1[6-9]|2[0-7])\.|141\.101\.|162\.158\.|108\.162\."
                r"|173\.245\.|188\.114\.|190\.93\.|198\.41\.|131\.0\.7[2-5]\.|103\.(21|22|31)\."
                r"|2400:cb00|2606:4700|2803:f800|2405:b500|2405:8100|2a06:98c0|2c0f:f248)")
LINE = re.compile(r"^\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):.*?\[Client ([0-9a-fA-F.:]+)\]")
MONTHS = {m: i for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}

hours = int(sys.argv[1])
total, cloudflare = collections.Counter(), collections.Counter()
for line in sys.stdin:
    m = LINE.match(line)
    if not m:
        continue
    day, mon, year, hour, ip = m.groups()
    key = (int(year), MONTHS.get(mon, 0), int(day), int(hour))
    total[key] += 1
    if CF.match(ip):
        cloudflare[key] += 1

if not total:
    print("no lines found"); raise SystemExit(0)

print("%-18s%8s%8s%9s" % ("HOUR (UTC)", "LINES", "CF", "CF%"))
for key in sorted(total)[-hours:]:
    y, mo, d, h = key
    n, c = total[key], cloudflare[key]
    label = f"{d:02d}/{mo:02d}/{y} {h:02d}h"
    print("%-18s%8d%8d%8.1f%%" % (label, n, c, 100*c/n))
' "$HOURS"
