#!/usr/bin/env python3
"""Generate a synthetic NPM log seed.

This is what makes the repo runnable by someone with no access to the homelab.
CI builds against it, so a green run on a fork means the pipeline genuinely works
rather than that the tests were skipped.

The seed matches the *shape* of the real logs, not their content: all three line
formats, the same field layout, the same awkward cases. It deliberately includes
things that break naive parsers, because a seed where everything is clean tests
nothing:

  * a Cloudflare client address, so `client_trust` has both values
  * an IPv6 client
  * a line no format matches, so the reject path is exercised
  * TLS bytes sent to port 80, which parse as `combined` with no usable request
  * a multi-value `$upstream_status` from a retry
  * a query string carrying a token, which must not survive
  * a Host header that is an address, as scanners send
  * a day boundary in a non-UTC offset

Deterministic: the same seed value produces byte-identical files, so a CI failure
is reproducible locally.
"""

from __future__ import annotations

import argparse
import random
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path

HOSTS = {
    "primary": [
        "tv.example.test", "cloud.example.test", "home.example.test",
        "radarr.example.test", "auth.example.test", "s3.homelab.example.test",
    ],
    "secondary": ["camera.secondary.example.test"],
}

PATHS = [
    "/", "/api/v3/command", "/api/v3/queue/details?movieId=665", "/login",
    "/AiSearch/Health", "/index.php", "/wp-login.php", "/.env",
    "/static/app.js", "/api/attention-zones", "/live/segment-042.ts",
]

USER_AGENTS = [
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
     "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"),
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15",
    "curl/8.7.1",
    "python-requests/2.32.3",
    "Mozilla/5.0 (compatible; SomeBot/1.0; +http://example.test/bot)",
    "-",
]

# Cloudflare edges, so the seed exercises both sides of `client_trust`. Everything
# else is from RFC 5737 / RFC 3849 documentation ranges -- no real address is
# committed to this repository.
CDN_CLIENTS = ["172.71.126.253", "104.23.166.63", "141.101.68.93"]
VISITOR_CLIENTS = [
    "203.0.113.7", "203.0.113.42", "198.51.100.4", "198.51.100.77",
    "192.0.2.55", "2001:db8:1234:5678::1", "2001:db8:abcd::42",
]

STATUSES = [200] * 12 + [204, 301, 302, 304, 400, 401, 403, 404, 404, 499, 500, 502, 503]


def proxy_line(ts: datetime, host: str, path: str, client: str, status: int,
               ua: str, length: int, upstream: str | None = None) -> str:
    stamp = ts.strftime("%d/%b/%Y:%H:%M:%S %z")
    upstream = upstream or str(status)
    return (
        f'[{stamp}] - {upstream} {status} - GET https {host} "{path}" '
        f'[Client {client}] [Length {length}] [Gzip -] [Sent-to 192.0.2.10] '
        f'"{ua}" "-"'
    )


def standard_line(ts: datetime, host: str, path: str, client: str, status: int,
                  ua: str, length: int) -> str:
    stamp = ts.strftime("%d/%b/%Y:%H:%M:%S %z")
    return (
        f'[{stamp}] {status} - GET http {host} "{path}" '
        f'[Client {client}] [Length {length}] [Gzip -] "{ua}" "-"'
    )


def combined_line(ts: datetime, client: str, request: str, status: int,
                  ua: str, length: int) -> str:
    stamp = ts.strftime("%d/%b/%Y:%H:%M:%S %z")
    return f'{client} - - [{stamp}] "{request}" {status} {length} "-" "{ua}"'


def generate_day(rng: random.Random, day: date, instance: str, count: int) -> dict[str, list[str]]:
    """One day of logs for one instance, split across the files NPM would use."""
    vhosts = HOSTS[instance]
    files: dict[str, list[str]] = {f"proxy-host-{i}_access.log": [] for i in range(1, len(vhosts) + 1)}
    files["default-host_access.log"] = []
    files["fallback_access.log"] = []

    for _ in range(count):
        ts = datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(
            seconds=rng.randrange(86400)
        )
        idx = rng.randrange(len(vhosts))
        vhost = vhosts[idx]
        path = rng.choice(PATHS)
        ua = rng.choice(USER_AGENTS)
        status = rng.choice(STATUSES)
        length = rng.randrange(0, 60000)
        # Most traffic is post-cutover and shows a real client; a slice is left as a
        # CDN edge so the mart's `resolved`-only visitor metric has something to exclude.
        client = rng.choice(CDN_CLIENTS) if rng.random() < 0.15 else rng.choice(VISITOR_CLIENTS)
        files[f"proxy-host-{idx + 1}_access.log"].append(
            proxy_line(ts, vhost, path, client, status, ua, length)
        )

    base = datetime.combine(day, datetime.min.time(), tzinfo=UTC)

    # --- the awkward cases, one of each, always present -----------------------
    files["fallback_access.log"] += [
        # Host header is an address: a scanner probing the IP directly.
        standard_line(base + timedelta(hours=3), "192.0.2.1", "/", "198.51.100.4", 400,
                      "Mozilla/5.0 (compatible; ScanBot/1.0)", 252),
        # Zero-padded address as a Host header, exactly as seen in the real logs.
        standard_line(base + timedelta(hours=4), "192.000.002.105", "/", "203.0.113.7", 400,
                      "-", 0),
    ]
    files["default-host_access.log"] += [
        # TLS handshake bytes sent to port 80: parses as combined, no usable request.
        combined_line(base + timedelta(hours=5), "203.0.113.9",
                      r"\x16\x03\x02\x00\x9c\x01", 400, "-", 157),
        combined_line(base + timedelta(hours=6), "198.51.100.4",
                      "GET /wp-login.php HTTP/1.1", 404, "Mozilla/5.0 (Windows NT 10.0)", 183),
    ]
    first = "proxy-host-1_access.log"
    files[first] += [
        # A query string carrying a token. Must not survive into the warehouse.
        proxy_line(base + timedelta(hours=7), vhosts[0], "/reset?token=hunter2-secret",
                   "203.0.113.42", 200, USER_AGENTS[0], 512),
        # nginx retried upstream: $upstream_status holds two values.
        proxy_line(base + timedelta(hours=8), vhosts[0], "/api/v3/command",
                   "203.0.113.7", 200, USER_AGENTS[0], 1594, upstream="502, 200"),
        # IPv6 visitor.
        proxy_line(base + timedelta(hours=9), vhosts[0], "/",
                   "2001:db8:1234:5678::1", 200, USER_AGENTS[1], 837),
        # A non-UTC offset landing on the same UTC day.
        proxy_line((base + timedelta(hours=23, minutes=30)).astimezone(
            timezone(timedelta(hours=2))), vhosts[0], "/late",
            "203.0.113.42", 200, USER_AGENTS[0], 64),
        # A line no format matches. The reject path must record it, not drop it.
        "",  # placeholder replaced below so it is not mistaken for a blank line
    ]
    files[first][-1] = "!!! not an access log line at all -- NPM never wrote this"

    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="seed/logs", help="output root (default: seed/logs)")
    parser.add_argument("--days", type=int, default=3, help="days to generate (default: 3)")
    parser.add_argument("--end", default=None, help="last day, YYYY-MM-DD (default: 2026-08-27)")
    parser.add_argument("--seed", type=int, default=20260827, help="RNG seed for determinism")
    parser.add_argument("--per-day", type=int, default=400, help="requests per day per instance")
    args = parser.parse_args()

    end = date.fromisoformat(args.end) if args.end else date(2026, 8, 27)
    # Deterministic by design: the same --seed must produce byte-identical files
    # so a CI failure reproduces locally. Nothing here is a secret.
    rng = random.Random(args.seed)  # noqa: S311
    root = Path(args.out)

    written = 0
    for instance in HOSTS:
        target = root / instance
        target.mkdir(parents=True, exist_ok=True)
        merged: dict[str, list[str]] = {}
        for offset in range(args.days):
            day = end - timedelta(days=args.days - 1 - offset)
            for name, lines in generate_day(rng, day, instance, args.per_day).items():
                merged.setdefault(name, []).extend(lines)
        for name, lines in merged.items():
            if not lines:
                continue
            (target / name).write_text("\n".join(lines) + "\n")
            written += len(lines)
        print(f"{instance}: {len(merged)} files, {sum(len(v) for v in merged.values())} lines")

    print(f"seed written to {root.resolve()} ({written} lines total)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
