# Implementation plan

How `PLAN.md` actually gets built. That file is the *why*; this one is the *how*.

Written 2026-08-27, then corrected the same day against the real servers over SSH. Every
number and path below was read off the boxes, not remembered. `PLAN.md` still governs the
goals — §1 (why the repo exists), §5 (published vs private), §6 (the checklist).

---

## 1. What checking the servers changed

[Private host inventory, source measurements, and access instructions omitted.]

---

## 2. The two hosts

```
primary  (ssh.example.test)  Ubuntu 22.04       secondary (ssh-secondary.example.test)  Ubuntu 26.04
├── npm-npm-1     jc21/nginx-proxy-manager    ├── npm-npm-1   jc21/nginx-proxy-manager
│   44 proxy hosts · [count withheld] · ~10-[count withheld] lines/day  │   1 proxy host · [count withheld] · 28-[count withheld] lines/day
├── homelab-minio  bucket: camera-history     └── homelab-camera   ← the event source
├── jellyfin       10.11.8 "Homelab TV"           /srv/projects/camera/
├── keycloak       26.7.2, realm: homelab                camera-app
└── grafana, prometheus, kafka, hadoop…
```

Both reached through Cloudflare Access — `~/.ssh/config` already has the `ProxyCommand`
entries, so `ssh ssh.example.test` just works.

**The datalake stack runs on `primary`.** MinIO, Jellyfin and Keycloak are all local to it, and
it is the machine with room. It reaches the camera host over HTTPS through the existing
`camera.secondary.example.test` proxy — no new tunnel, no new exposed port.

The awkward one is `secondary`'s NPM logs, which are on the *other* box. Options, in the
order they should be tried: a nightly `rsync` over the existing SSH path (simple, one cron
entry, one direction, no new service), or a second Dagster code location on that host (correct,
and more than Stage 1 needs). **Start with rsync.** Say so in the README, and say why.

> **Live issue, unrelated to this project:** `homelab-camera` is currently `Exited (1)` and
> restart-looping — ffmpeg cannot reach the RTSP camera at `192.0.2.10:18554`
> (connection refused). Stage 2b needs it running. Worth a look regardless.

---

## 3. How to connect, verify and modify each project

Everything here was run today and works. Prefixes: `F$` on primary, `A$` on secondary.

### Nginx Proxy Manager

| | primary | secondary |
|---|---|---|
| Container | `npm-npm-1` | `npm-npm-1` |
| Host data path | `/srv/npm/data` | `/srv/npm/data` |
| Logs | `<data>/logs` → `/data/logs` | same |

```bash
ssh ssh.example.test 'docker exec npm-npm-1 ls -la /data/logs | head'
ssh ssh.example.test 'docker exec npm-npm-1 cat /etc/nginx/conf.d/include/log.conf'
ssh ssh-secondary.example.test 'docker exec npm-npm-1 sh -c "cat /data/logs/*_access.log | wc -l"'
```

**To modify:** nothing needs modifying for Stage 1 — the pipeline only reads. Mount
`/srv/npm/data/logs` **read-only** into Dagster. Read-only is not a formality: a
pipeline that can corrupt the reverse proxy's logs is a pipeline that can take the house
offline.

The one change worth making later is real-IP restoration (§4), and that is a per-proxy-host
edit in the NPM web UI under *Advanced*, not a file edit. Changing it rewrites the meaning of a
column, so it is a decision with a date, recorded in the README.

### MinIO

Volume `homelab-camera-analysis_minio-data`, network `homelab-analysis`, ports 9000/9001.
Public at `https://s3.homelab.example.test` (health check returns 200). Today it holds exactly
one bucket, `camera-history`.

```bash
ssh ssh.example.test 'docker exec homelab-minio ls /data'
curl -s -o /dev/null -w "%{http_code}\n" https://s3.homelab.example.test/minio/health/live
```

**To modify:** create a `datalake` bucket and a **separate** access key scoped to it. One key
per consumer — the camera archive's key must not be reused, so that revoking one never breaks
the other. Do it in the MinIO console on 9001, and record the policy JSON in this repo (the
policy is not a secret; the key is).

### Jellyfin

10.11.8, "Homelab TV", container port 8096 → **host 8097**, network `primary_tv`.

```bash
ssh ssh.example.test 'curl -s http://127.0.0.1:8097/System/Info/Public'
```

**To modify:** create an API key in *Dashboard → API Keys* named `datalake`. That is the only
change; everything else is reads. Verify it with:

```bash
ssh ssh.example.test 'curl -s -H "Authorization: MediaBrowser Token=\"$KEY\"" \
  "http://127.0.0.1:8097/Items?Recursive=true&IncludeItemTypes=Movie&Limit=1"'
```

### Keycloak

26.7.2, realm `homelab`, network `primary_reseau`. Discovery returns 200 at
`https://auth.example.test/realms/homelab/.well-known/openid-configuration`.

**To modify** (Stage 2b, admin console):
1. New client `datalake-exporter` — confidential, **service accounts on**, standard flow
   **off**. It is not a login; it is a program.
2. Client role `export:read`, assigned to that client's service account.
3. Hand the client id and secret to Dagster; nothing else.

Verify the whole thing before writing a line of camera code:

```bash
curl -s -d grant_type=client_credentials -d client_id=datalake-exporter \
  -d client_secret=$SECRET \
  https://auth.example.test/realms/homelab/protocol/openid-connect/token
```

### homelab-camera

Host `secondary`, compose dir `/srv/projects/camera/camera-app`,
data volume `<dir>/data` → `/app/data`. Public at `https://camera.secondary.example.test`.
Local checkout for editing: `~/dev/homelab_camera`.

```bash
ssh ssh-secondary.example.test 'docker ps -a --filter name=homelab-camera'
ssh ssh-secondary.example.test 'ls -la /srv/projects/camera/camera-app/data'
```

**To modify** — the only source that needs real code written in another repo. In its own PR,
in this order:

1. Add `jose`. The app has no JWT library today.
2. A bearer middleware that fetches the realm JWKS, caches it, verifies signature, `iss` and
   `exp`, and requires the `export:read` role.
3. Mount it on `/api/export/*` **only**. Existing session auth keeps guarding everything else.
4. Deploy: `git pull && docker compose up -d --build` in the compose dir.

Signature verification is not optional and is worth stating plainly in that repo's commit
message. `auth-service.js` today reads JWT claims without verifying them — fine for a token
that came straight from Keycloak over TLS during the login redirect, **wrong** for a token a
caller hands you. Without verification anyone who can reach the endpoint writes their own.

---

## 4. Source A — Nginx Proxy Manager logs

### Three formats, not one

NPM declares two (`/etc/nginx/conf.d/include/log.conf`) and inherits a third:

```
proxy:    [$time_local] $upstream_cache_status $upstream_status $status - $request_method
          $scheme $host "$request_uri" [Client $remote_addr] [Length $body_bytes_sent]
          [Gzip $gzip_ratio] [Sent-to $server] "$http_user_agent" "$http_referer"
standard: same, without the two upstream fields and without Sent-to
combined:  stock nginx — default-host_access.log only
```

Real lines, one from each host:

```
[27/Aug/2026:20:42:55 +0000] - 200 200 - GET https tv.example.test "/AiSearch/Health"
  [Client 172.71.130.17] [Length 97] [Gzip -] [Sent-to 192.0.2.26] "Mozilla/5.0 …" "-"
198.51.100.34 - - [24/Aug/2026:09:04:12 +0000] "\x16\x03\x02\…
```

The parser dispatches on shape: a line starting with `[` is `proxy` or `standard` (told apart
by field count), anything else is `combined`. Unparseable lines go to a
`_rejects` table with the raw text, and a dbt test fails the build if the reject rate crosses a
threshold. Silently dropping lines you can't parse is how a pipeline lies.

### The client IP is usually not a client

**[rate withheld] of primary's `Client` values are Cloudflare edge addresses** ([count withheld] / [count withheld]). Traffic
arrives through Cloudflare, real-IP restoration was never configured, so `$remote_addr` is
whichever Cloudflare machine forwarded it. `secondary` shows real ISP addresses, because
that proxy host isn't fronted the same way.

So the same column means different things depending on which host and which proxy host the row
came from. Everything downstream has to respect that:

- A `client_ip_trust` column — `direct` or `cdn_edge` — derived from whether the address falls
  in a published Cloudflare range. Never inferred per row at query time; decided once, at
  ingestion.
- Distinct-visitor metrics are computed over `direct` rows only, and the mart says so in its
  description. A visitor count that quietly averages real users with CDN nodes is a wrong
  number presented confidently, which is worse than no number.
- The fix, when it's worth it, is `set_real_ip_from` with Cloudflare's ranges in each proxy
  host's Advanced tab. Deliberately *not* Stage 1: it changes the meaning of history, and
  having both eras in one table — with a dated cutover documented — is a better story than
  pretending the problem never existed.

This is the "partitioning decision defended with real before/after numbers" line on the
checklist, except it's a data-quality decision, which is better. It was found by reading the
data, and that is exactly what the checklist is trying to detect.

### Partitioning

The daily partition is defined by **the timestamp inside each line**, never by the filename.
NPM names files by proxy-host id; logrotate moves them weekly and gzips them. A partition run
for `dt=D` reads the live logs plus every rotated sibling (`.1.gz` … `.4.gz`), parses each
line's `$time_local`, and keeps only that day.

Slower than trusting a filename, and the only version that survives a rotation landing
mid-run. It is also what makes a re-run reproducible: same date filter, same lines, same
output, no matter which file they were found in.

**Retention: weekly rotation, keep 4 → about five weeks, and 27 July 2026 is the oldest line
on the box today.** Backfill cannot reach further back than that, ever. Once the pipeline
runs, the lake becomes the only long-term history — which is a real reason for this project
to exist, and belongs in the README.

### Landing

`s3://datalake/raw/npm_access/host=<primary|secondary>/dt=YYYY-MM-DD/data.parquet`

One object per host per day. One object means one PUT, so the write is atomic without needing
rename semantics S3 doesn't have. Re-running overwrites it. That is the entire idempotency
story for this source, and it fits in a sentence.

---

## 5. Source B — camera events

The most work, and the source that justifies the project. Everything else is tabular data
being moved; this one is untrusted model output being made trustworthy.

Keycloak setup and the middleware are in §3. The endpoints:

```
GET /api/export/events?from=<iso>&to=<iso>   attention events
GET /api/export/clips?from=<iso>&to=<iso>    clip metadata
GET /api/export/logs?from=<iso>&to=<iso>     event log entries
```

NDJSON, read-only. The contract is the part that matters:

- **`[from, to)`** half-open, so adjacent partitions can't double-count or drop a boundary row.
- **Ordered by timestamp then id** — deterministic output, keyset pagination for free.
- **No media.** No playlist URLs, thumbnails or clip paths.
- **No zone geometry.** `zoneId` and `zoneName` go; the polygon points stay home. Those
  coordinates are a map of the property, and `homelab-camera` already argues against handing
  that over. An interviewer who spots the contradiction reads it as not having thought about it.
- **No user identity.** `createdBy` collapses to a stable hash. Analytics needs
  manual-vs-automatic, not a name.

### Late-arriving data — the honest bit

Vision analysis is asynchronous. An event lands with `confidence`, `summary` and `lifecycle`
empty and gets them minutes or hours later. Re-extracting yesterday tomorrow legitimately
returns *different* rows than it did today.

So "re-running a partition replaces it exactly" needs its precise form: **re-running replaces a
partition with the current truth for that date, and the marts always reflect the latest version
of each event.** Raw rows carry `_extracted_at`; the mart keeps the newest per `event_id`.
Nothing is silently overwritten and no analysis result is lost to a race.

This is the best interview answer in the repo. Write it down while it's fresh.

### Validating the model's output

`analysis-service.js` already does the hard part — fixed species vocabulary, unknown species
rewritten to `autre`, sightings under 0.6 confidence dropped, and a *missing* confidence
deliberately distinguished from a *low* one. The pipeline's job is to keep that true, in dbt:

- `accepted_values` on `species` against the same vocabulary list
- `accepted_values` on `category` and `event_type`
- `confidence` is null or within `[0, 1]` — null is a real state, not a failure
- `not_null` on ids, `unique` on `event_id` in the mart

If the model starts returning a species the app doesn't know, the build fails. That is the
"tests that fail the pipeline, not just log" line, backed by a real case rather than a
`not_null` on a primary key.

---

## 6. Source C — Jellyfin

```
GET /Items?Recursive=true&IncludeItemTypes=Movie
    &Fields=Genres,ProviderIds,ProductionYear,CommunityRating,RunTimeTicks,DateCreated,MediaStreams
    &StartIndex=<n>&Limit=500
```

Metadata only. No `/Sessions`, no `/Users`, no playback history — `PLAN.md` §2 settled that.
`Path` is dropped too: filesystem layout is topology, not analysis.

~[count withheld] items is a full snapshot per day, well under a second. Incremental sync would be
premature on a dataset this size, and snapshots are what SCD2 wants anyway.

`ProviderIds.Tmdb` comes back as a real id, so enrichment is `GET /movie/{tmdb_id}` — exact,
cheap, cacheable. The CSV had only a title, which means fuzzy matching, which means wrong
answers on remakes and translated titles. That difference alone justified the switch.

**The model.** The one place a star schema is genuinely the right shape rather than a
demonstration: `fct_library_title` (one row per movie per snapshot date), `dim_genre` plus
`bridge_title_genre` (genres are many-per-title; a bridge is the honest answer, and explaining
why beats flattening into `genre_1, genre_2, genre_3`), `dim_decade`, `dim_resolution_class`,
`dim_codec`. SCD2 over the daily snapshots gives library history — when a title arrived, when a
rating moved, when a file was re-encoded — free from snapshots you're taking anyway.

---

## 7. Warehouse

DuckDB on a named Docker volume; dbt (`dbt-duckdb`) reads Parquet from MinIO through httpfs and
models `staging → intermediate → marts`.

DuckDB rather than Postgres because the whole warehouse is one file that can be copied,
inspected and thrown away, and because it reads Parquet from S3 natively. At this volume a
server-based warehouse is a container to maintain with no benefit.

The DuckDB file is a build artefact. Lose it and `dagster job execute` + `dbt build` rebuilds it
from MinIO. Nothing in it is a source of truth — worth saying out loud, because it sets what
needs backing up (the bucket) and what doesn't (everything else).

Grafana already runs on `primary`, so the dashboard layer is a datasource away. Evidence.dev
can wait until there's something worth publishing screenshots of.

---

## 8. Orchestration

**Assets.** `DailyPartitionsDefinition` per source. `RetryPolicy(max_retries=3,
backoff=EXPONENTIAL)` on the two that cross a network. NPM's reads a mounted file and should
fail loudly instead — a missing mount won't fix itself, and retrying hides it.

**Schedules.** One daily run at 03:00 for yesterday's partitions, after the rsync from
`secondary` has landed.

**Sensors — two, doing different jobs, which is the point.**

1. A polling sensor on the camera export's newest event timestamp; new events re-materialise
   today's partition. This is the intra-day path, and why late-arriving analysis results appear
   without waiting for 03:00.
2. A `@run_failure_sensor` pushing to the notification channel `homelab_camera` already uses.
   Alerting on failure is on the checklist, and reusing an existing channel beats inventing one.

`PLAN.md` wanted a MinIO object sensor. Dropped: the camera S3 archive is off and
`camera-history` is the only bucket, so it would watch nothing. A polling sensor against a real
API is honest; a bucket sensor that never fires is decoration. Add it if archiving is switched
on later.

**Backfill.** Dagster's dialog covers it, bounded by the five-week log retention in §4. The
checklist wants a *documented case where it was used* — the first camera export schema change
is the natural one. Capture it when it happens rather than inventing one.

---

## 9. Privacy, in one place

| Field | Treatment | Why |
|---|---|---|
| Client IP | HMAC-SHA256 + salt, truncated; plus /24 or /48 net | Personal data under GDPR. Hashed **before** the write — there is no raw landing zone, because that zone is the leak. |
| Client IP trust | `direct` vs `cdn_edge` column | [rate withheld] are CDN edges. Mixing them silently produces confident wrong numbers. |
| Query strings, Referer | Dropped | Carry tokens, reset links and session ids often enough not to gamble. |
| User-agent | Kept | Not identifying on its own, and it's how bots are separated from browsers. |
| Zone polygons | Never exported | A map of the property. Consistent with `homelab-camera`. |
| Clip URLs, thumbnails | Never exported | Media stays on the server. |
| `createdBy` | Hashed to a stable id | Analytics needs manual-vs-automatic, not a name. |
| Jellyfin `Path`, `/Sessions` | Never requested | Topology and viewing habits. Neither adds anything. |
| Real data of any kind | Never published | `PLAN.md` §5. The repo ships code, seed and screenshots. |

Rotating the IP salt breaks continuity in visitor counts. That is correct behaviour, and it
goes in the README.

---

## 10. CI and the synthetic seed

The seed generator is what makes this repo runnable by a stranger, so it is built in Stage 1,
not bolted on later.

A script writing fake data in the same *shapes* as the real thing — all three NPM line formats,
camera events with the real vocabulary and the real null patterns, movies in Jellyfin's JSON
shape. Deterministic, committed as fixtures.

GitHub Actions on every push: generate seed → land it → `dbt build` → `dagster definitions
validate`. No secrets, no network, no server. Green on a fork means the repo is real.

Make the seed exercise the awkward cases: a Cloudflare client IP, an unparseable line, a null
confidence, a species outside the vocabulary, an IPv6 client, a title with no TMDB id. A seed
where everything is clean tests nothing.

---

## 11. Build order

Each stage ends somewhere publishable. The failure mode is an ambitious repo that's [rate withheld] done
when someone asks for the link.

**Stage 1 — NPM logs, both hosts, end to end.** Compose stack on `primary`, rsync from
`secondary`, the three-format parser with a reject table, the hash-at-the-boundary step
and the `client_ip_trust` column, MinIO landing, dbt staging + one mart, tests, seed generator,
CI, README with the Dagster UI screenshot and the [rate withheld] figure. No camera, no Jellyfin, no
Keycloak. **Stop and publish.**

**Stage 2a — Jellyfin.** Pure addition: API key, snapshot asset, star schema. Touches no other
repo, so it lands fast and proves the platform takes a second source cleanly.

**Stage 2b — camera.** Touches three systems, so in order, each verifiable alone: Keycloak
client → bearer middleware in `homelab_camera` (own PR, own tests) → `/api/export/*` → Dagster
asset → dbt models and vocabulary tests. Needs the container healthy again first.

**Stage 3 — MCP server.** Unchanged from `PLAN.md` §7. Typed tools over the marts, read-only
DuckDB, row caps and timeouts, `.xlsx` output. The headline, and the demo video.

---

## 12. Still open

Small, and none of it blocks Stage 1.

1. **MinIO disk headroom** on the `homelab-camera-analysis_minio-data` volume. Parquet from
   ~[volume withheld]/week of logs is small, but check before assuming.
2. **Which of secondary's rotated logs survive the rsync window.** If the box is down for
   more than a week, a rotation is lost for good. Nightly rsync makes it a non-issue; confirm
   the cron actually runs.
3. **Whether `default-host` and `fallback` logs are worth ingesting at all.** They're scanner
   noise and TLS junk. Probably a separate, cheaper table — decide with the data in hand.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
