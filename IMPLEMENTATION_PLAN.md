# Implementation plan

How `PLAN.md` actually gets built. That file is the *why*; this one is the *how*.

Written 2026-08-27 and corrected the same day against the real servers over SSH. Every number
and path below was read off the boxes. `PLAN.md` still governs the goals — §1 (why the repo
exists), §5 (published vs private), §6 (the checklist).

---

## 1. What checking the servers changed

**There are two hosts and two Nginx Proxy Managers, not one of each.** `fraserv` runs NPM,
MinIO, Jellyfin and Keycloak. `amblevilleserv` runs the camera — and a second NPM which fronts
one proxy host but logs **28k–115k lines/day** against fraserv's 10–15k, because HLS streaming
is thousands of segment requests per session. The instance that looked like an afterthought
holds most of the data.

**86% of the client IP column is a Cloudflare edge, not a visitor** (34,258 of 39,961 lines).
Real-IP restoration was never configured. This is now being fixed rather than worked around —
see §6.

**Rotation is weekly, keep 4** — not daily. About five weeks of history, and 27 July 2026 is
the oldest line on the box. Backfill has a hard floor.

**Camera events aren't in MinIO.** `ARCHIVE_S3_ENABLED=false` and `camera-history` is the only
bucket. The event store is JSON on the camera host, so the export has to be built.

**The box already contains far more than three sources.** 54 containers, a 9-million-row
telemetry table, and an LLM spend ledger. §4 goes through all of it and decides what earns a
place. Two of the finds change the shape of the project.

---

## 2. The estate

```
fraserv  (ssh.fraserv.fr)  Ubuntu 22.04      amblevilleserv (sshamb.fraserv.fr) Ubuntu 26.04
├── npm-npm-1     44 hosts · ~10-15k/day     ├── npm-npm-1   1 host · 28-115k lines/day
├── homelab-minio  bucket: camera-history    └── homelab-camera   ← the event source
├── jellyfin       10.11.8, host port 8097       /home/francois/projects/camera/
├── keycloak       26.7.2, realm: homelab               cameraTestLiveFeed
├── postgresql     ai_platform 8.7G · fraserv_homelab 3.2G · litellm 72M
├── prometheus     1.6G TSDB, 15-day default retention
├── kafka + zookeeper + kafka-ui   2 topics
├── docker-hadoop  namenode/datanode/RM/NM/history — 168 KB stored
└── grafana, jenkins, nextcloud, radarr, sonarr, … (54 containers total)
```

Both reached through Cloudflare Access; `~/.ssh/config` already has the `ProxyCommand` entries.

**The datalake stack runs on `fraserv`** — MinIO, Jellyfin, Keycloak and Postgres are all local
to it. It reaches the camera over HTTPS through the existing `camera.amb.fraserv.fr` proxy: no
new tunnel, no new exposed port. `amblevilleserv`'s NPM logs come across by nightly `rsync`
over the existing SSH path — one cron entry, one direction, no new service. A second Dagster
code location on that host would be more correct and is more than Stage 1 needs.

> **Live issue, unrelated:** `homelab-camera` is `Exited (1)` and restart-looping — ffmpeg
> cannot reach the RTSP camera at `172.20.0.1:18554`. Stage 2b needs it running.

---

## 3. Design choices, and what was rejected

Every one of these has a cheaper-looking alternative already running on the box. That is what
makes them worth writing down.

### Why Parquet files, and not HDFS + Spark

There is a Hadoop cluster on `fraserv` right now: namenode, datanode, resourcemanager,
nodemanager, historyserver. Five containers. **It stores 168 KB** — one YARN state directory,
nothing else, after two years.

That is the whole argument, and it is sitting on the machine.

- **The volume isn't there.** All sources together are roughly 50 MB/day, ~18 GB/year. Spark's
  value begins where one machine ends. Here, JVM and executor startup would cost more wall time
  than the actual computation.
- **HDFS solves a problem this box doesn't have.** Block replication across racks protects
  against commodity-disk failure at scale. One host, one disk: it is durability theatre, and it
  costs five containers of RAM.
- **Parquet is the portable half of that stack, and it's free.** Columnar, compressed,
  predicate pushdown, and readable by Spark, DuckDB, pandas and Polars alike. Choosing Parquet
  keeps the Spark door open without paying rent on it. If volume 100×'s, point Spark at the
  same files and change nothing else.
- **For the CV specifically:** Spark and Hadoop are already on it from SG. Doing them again
  proves nothing new. Knowing when *not* to reach for them, and being able to show the empty
  cluster that proves the point, is the more senior signal.

### Why DuckDB

- One file. No server, no port, no user management, no backup story of its own — it is a build
  artefact, rebuildable from MinIO.
- Reads Parquet straight from S3 via `httpfs`, so exploration needs no load step.
- Columnar and vectorised, which is exactly the shape of the workload: aggregates over a date
  range.
- **Not Postgres**, even though it is already running and free. It is row-oriented, so
  `count(*) group by day` over ten million rows reads every row. Worse, it is the shared
  database behind Keycloak, the AI platform and banking — putting analytical scans on it means
  a bad query has a blast radius that includes the front door lock.
- **Not ClickHouse.** Right answer at 100× this volume; another server to operate at this one.
- **Honest limitation:** DuckDB is single-writer. Concurrent asset writes must be serialised.
  Free at daily-batch cadence; it would hurt under streaming, which is the next section.

### Why Dagster

- **Assets, not tasks.** You declare that a table exists and what it derives from; Dagster
  works out order, tracks freshness per partition and shows lineage. Airflow makes you say "run
  this script at 3am" and keep the state yourself.
- **Partitions are first-class.** `DailyPartitionsDefinition` gives per-day status, a backfill
  UI, and re-running exactly one day. §6's rotation problem needs precisely that.
- **The UI is the ingestion IHM** that `PLAN.md` §4 already decided not to build.
- **Local dev is real.** `dagster dev` runs the whole graph on the laptop against the seed.
- **Not Airflow:** heavier, task-centric, and the CV already has Control-M. Same category, no
  new signal.
- **Not cron plus scripts:** no partition state, no backfill, no lineage — the exact line
  `PLAN.md` §3 draws between a pipeline and a tutorial.
- **Honest cost:** the asset model takes a day to click if you've only used task schedulers.

### Why daily batch, and not streaming or socket pushes

The strongest argument here is also on the box, and it is your own code.

`homelab_homeapp` already streams: Kafka → Socket.io → Postgres, three containers
(kafka, zookeeper, kafka-ui). What it has produced is the `Log` table —
**9,007,079 rows, 3.15 GB, of which 99.7% are CPU and RAM samples** nobody has ever queried.
The broker carries two topics. Streaming was built, and what it delivered was volume.

That is what streaming costs when the questions are daily questions. And they are:

> *How much traffic did each service get last month? Which animals crossed the garden this
> week? What did LLM calls cost in August? Which titles were added this quarter?*

Every one is answered at day grain. A stream would deliver the same answers, later, after more
plumbing. **Latency requirements drive architecture, and there isn't one here.**

Three more reasons, in order of how much they matter:

1. **Batch is re-runnable; a stream is not.** `PLAN.md`'s core property — re-running a
   partition replaces it — is one line in batch. In streaming it is exactly-once delivery,
   watermarks and state stores. Choosing batch is choosing to be able to fix yesterday.
2. **Real-time alerting already exists, in the right place.** The camera app alerts on its own
   events, live, where the context is. A lake that also tries to alert is a lake that is a bad
   alerting system. Analysis and reaction are different jobs.
3. **The one place freshness matters is handled without a broker.** Vision analysis lands
   hours late (§7), so a polling sensor re-materialises today's partition when new events
   appear. Hours, not days, and no new infrastructure.

**The escape hatch is honest and cheap:** Kafka is already running. If a question ever appears
that genuinely needs sub-minute data, add a streaming asset for that one source. Building the
whole platform around a requirement that does not exist is how you end up with nine million
rows of CPU samples.

---

## 4. What is worth keeping, and what isn't

54 containers. Most hold nothing worth a warehouse; a few hold more than the three sources
already planned. Verdicts below, with the numbers they were based on.

### Take it

| Source | Size / rate | Why it earns a place |
|---|---|---|
| **NPM access logs** (both hosts) | ~125k lines/day | The volume source, and real visitor IPs after §6. Geography, ASN, bot-vs-human, per-service audience. |
| **`litellm.LiteLLM_SpendLogs`** | 33,155 calls · €57.25 · Jun 16 → now | Every LLM call with model, tokens, latency and cost. Small, dense, no PII, and the perfect MCP demo: *"what did I spend on Claude in August?"* → `.xlsx`. |
| **Camera events** | thousands | The semi-structured showcase. Untrusted model output made trustworthy. |
| **Jellyfin library** | 2,422 titles | The dimensional showcase. Star schema, TMDB enrichment. |

The LiteLLM ledger was the best find of the survey. It was not in `PLAN.md`, it needs no export
to be written, and it is the one dataset where the MCP layer's value is obvious in a sentence.
**Promote it into Stage 2a**, ahead of Jellyfin.

### Take part of it

| Source | Take | Leave |
|---|---|---|
| **`ai_platform.usage_events`** (26k rows, 8.5 GB) | `requested_model`, `actual_model`, tokens, `cost_eur`, `latency_ms`, `status`, `failure_code`, `source` | `prompt` and `response`. They are the 8.5 GB — ~330 KB a row — and they are personal content. The useful part is about 5 MB. |
| **`fraserv_homelab.Log`** (9.0M rows, 3.15 GB) | The 0.3% that is actually events: 15,883 `HTTP_METRIC`, 2,883 `DB_METRIC`, 2,806 `SOCKET_EVENT`, 2,250 `CV_EVENT`, 164 `AUTH_EVENT` | 8,978,961 `SYSTEM_METRIC` rows. They are a worse copy of Prometheus, sampled by an app instead of a purpose-built scraper. Carries `ip`, `email`, `username` — §11 applies. |
| **Prometheus** | Daily rollups per instance and metric | The raw 15-second series. |
| **status-page `incidents.json`** | Incidents and uptime samples (24 MB, 90-day retention) | Nothing much — it is small and high-signal. |

Two of these are worth the paragraph they will get in an interview. `usage_events` is
**select the columns, leave the payload** — the same table is 8.5 GB or 5 MB depending on
whether you thought about it. And Prometheus keeps **15 days by default**; rolling it into the
lake turns fifteen days into forever, which is a real reason for a warehouse to exist rather
than a decorative one. status-page joins to NPM logs on time and answers "did traffic drop
because nobody came, or because it was down?"

### Leave it

- **`Log`'s `SYSTEM_METRIC` rows** — 9M rows duplicating Prometheus, badly. The single biggest
  table on the box, and the correct amount to ingest is zero.
- **`ai_platform` prompts and responses, `banking_tracker`, Nextcloud** — personal content and
  financial records. Not a close call.
- **wg-easy** — VPN peers are who connects from where. Network topology.
- **Authelia / Keycloak auth logs** — login patterns are genuinely interesting and it is a
  credential-adjacent audit trail. Bad risk-to-reward for a repo whose point is being shown to
  strangers.
- **qbittorrent, jackett, radarr, sonarr** — download history. Awkward to publish anything
  derived from, aggregates included. Not worth the paragraph.
- **Jenkins** (2 jobs, 32 MB), **syncthing, portainer, ombi, jellyseerr** — real but thin, or
  duplicative. Revisit if a stage runs short.

### And within the three planned sources

**NPM — keep:** timestamp, host, path, method, scheme, status, upstream status, bytes sent,
cache status, user-agent, upstream target, plus geography and ASN derived at ingestion.
**Drop:** query strings and referer (they carry tokens, reset links and session ids often
enough not to gamble), and the raw IP once enrichment has run (§6).

**Camera — keep:** event id, camera, zone id and name, rule type, lifecycle, start and
completion, confidence, category, species and counts, dismissal reason, duplicate-of, and the
French `summary`. **Drop:** zone polygon geometry, clip and playlist URLs, thumbnails, stream
ids, and `createdBy` identity. The polygons are a map of the property and `homelab-camera`
already argues against handing that over. The `summary` describes people at the house — fine in
the private warehouse, never in the published seed.

**Jellyfin — keep:** item id, name, production year, genres, community rating, runtime, codecs,
resolution, date added, TMDB id. **Drop:** `Path` (filesystem layout is topology), library
folder names, and `/Sessions` and `/Users` entirely. Playback history is personal and adds
nothing the metadata doesn't.

---

## 5. How to connect, verify and modify each project

Everything here was run today. `ssh ssh.fraserv.fr` and `ssh sshamb.fraserv.fr` work as-is.

### Nginx Proxy Manager (both hosts)

Container `npm-npm-1`; host data at `/home/docker/npm/data` → `/data`; logs in
`<data>/logs`.

```bash
ssh ssh.fraserv.fr 'docker exec npm-npm-1 cat /etc/nginx/conf.d/include/log.conf'
ssh sshamb.fraserv.fr 'docker exec npm-npm-1 sh -c "cat /data/logs/*_access.log | wc -l"'
```

**To modify:** one change, in §6 — the real-IP snippet. Mount `<data>/logs` **read-only** into
Dagster. Read-only is not a formality: a pipeline that can corrupt the reverse proxy's logs can
take the house offline.

### MinIO

Volume `homelab-camera-analysis_minio-data`, network `homelab-analysis`, ports 9000/9001,
public at `https://s3.homelab.fraserv.fr` (health returns 200). One bucket today,
`camera-history`.

**To modify:** create a `datalake` bucket and a **separate** access key scoped to it, in the
console on 9001. One key per consumer, so revoking one never breaks the other. Commit the
policy JSON to this repo — the policy is not a secret, the key is.

### Jellyfin

10.11.8 "Fraserv TV", container 8096 → **host 8097**, network `fraserv_tv`.

```bash
ssh ssh.fraserv.fr 'curl -s http://127.0.0.1:8097/System/Info/Public'
```

**To modify:** create an API key in *Dashboard → API Keys* named `datalake`. That is the only
change; everything else is reads.

### Postgres (LiteLLM, ai_platform, fraserv_homelab)

Container `postgresql_postgres_1`.

```bash
ssh ssh.fraserv.fr 'docker exec postgresql_postgres_1 psql -U postgres -c "\l+"'
```

**To modify:** create a role `datalake_ro` with `CONNECT` on `litellm`, `ai_platform` and
`fraserv_homelab` and `SELECT` on the specific tables in §4 — not `SELECT` on the schema. A
grant that names its tables is a grant that cannot quietly widen when someone adds a table full
of prompts next month.

### Keycloak

26.7.2, realm `homelab`, discovery returns 200 at
`https://auth.fraserv.fr/realms/homelab/.well-known/openid-configuration`.

**To modify** (Stage 2b, admin console): client `datalake-exporter`, confidential, **service
accounts on**, standard flow **off** — it is not a login, it is a program. Client role
`export:read` on its service account. Verify before writing any camera code:

```bash
curl -s -d grant_type=client_credentials -d client_id=datalake-exporter \
  -d client_secret=$SECRET \
  https://auth.fraserv.fr/realms/homelab/protocol/openid-connect/token
```

### homelab-camera

Host `amblevilleserv`, compose dir `/home/francois/projects/camera/cameraTestLiveFeed`, data at
`<dir>/data` → `/app/data`, public at `https://camera.amb.fraserv.fr`. Editing checkout:
`~/dev/homelab_camera`.

**To modify** — the only source needing real code in another repo. Own PR, in this order:

1. Add `jose`. The app has no JWT library today.
2. A bearer middleware: fetch the realm JWKS, cache it, verify signature, `iss` and `exp`, and
   require the `export:read` role.
3. Mount it on `/api/export/*` **only**. Session auth keeps guarding everything else.
4. `git pull && docker compose up -d --build` in the compose dir.

Signature verification is not optional. `auth-service.js` today reads JWT claims without
verifying them — correct for a token that arrived straight from Keycloak during a login
redirect, **wrong** for a token a caller hands you. Without it, anyone who reaches the endpoint
writes their own.

---

## 6. Source A — NPM logs, and getting real visitor IPs

### Restoring the real IP — do this first

Real IPs are wanted, and the fix is small. `nginx -V` confirms `--with-http_realip_module` is
compiled in, and `nginx.conf` already includes an optional global snippet at
`/data/nginx/custom/http_top.conf`. That directory does not exist yet; creating it applies the
change to **every proxy host at once**, and survives container updates because it lives in the
mounted volume.

```bash
# on both hosts, as root
mkdir -p /home/docker/npm/data/nginx/custom
{ curl -s https://www.cloudflare.com/ips-v4; curl -s https://www.cloudflare.com/ips-v6; } \
  | sed 's/^/set_real_ip_from /; s/$/;/' > /home/docker/npm/data/nginx/custom/http_top.conf
cat >> /home/docker/npm/data/nginx/custom/http_top.conf <<'EOF'
real_ip_header CF-Connecting-IP;
real_ip_recursive on;
EOF
docker exec npm-npm-1 nginx -t && docker exec npm-npm-1 nginx -s reload
```

Verify by watching a request from a known address appear as itself rather than as `172.71.x`.

Two things that matter more than the commands:

- **`set_real_ip_from` must list only Cloudflare's ranges.** Trust a wider source and anyone
  can forge `CF-Connecting-IP` and choose their own IP in your logs. This is the difference
  between restoring an identity and letting visitors invent one.
- **Fetch the ranges, don't paste them.** Cloudflare publishes them at the URLs above and they
  change. A `curl` in a quarterly checklist is the whole maintenance burden.

The log format needs no change: it already writes `$remote_addr`, and `real_ip` rewrites that
variable in place. The `[Client …]` field simply starts containing the truth.

### The cutover is a data event, not just a config change

Rows before and after mean different things, and that has to be visible in the data rather than
remembered. Every row carries `client_ip_trust` — `direct`, `cdn_edge`, or `restored` —
decided once at ingestion by testing the address against the live Cloudflare range list.
Distinct-visitor metrics run over `direct` and `restored` only, and the mart's description says
so. A visitor count that quietly averages real people with CDN nodes is a confident wrong
number, which is worse than no number.

The five weeks of history already on disk stay `cdn_edge` forever. Keeping both eras in one
table with a dated cutover is a better story than backfilling a fiction, and it is the
"partitioning decision defended with real before/after numbers" line on `PLAN.md`'s checklist —
except it is a data-quality decision, which is better, and it was found by reading the data.

### What real IPs unlock, without keeping the IPs

This is the part that makes the graphs good *and* keeps the privacy story intact.

**Enrich first, then discard.** While the address is in memory at the ingestion boundary,
derive country, city, ASN and network type from a local MaxMind GeoLite2 database — a file on
disk, not an API call, so no third party sees the traffic and there is no per-request cost.
Then hash the address and drop the original. The row keeps everything a chart needs and nothing
that identifies a household.

That yields, honestly: visitors by country and city, bot-versus-human by ASN (datacentre ranges
are not people), scanner and attack maps, per-service audience, returning-visitor rates from
the hash, and time-of-day patterns by region. All of it from columns derived at the boundary,
none of it from a stored identifier.

Real IPs are *more* sensitive than CDN edges, so the hashing discipline matters more now, not
less. There is no raw landing zone, because that zone is the leak.

### Three formats, not one

```
proxy:    [$time_local] $upstream_cache_status $upstream_status $status - $request_method
          $scheme $host "$request_uri" [Client $remote_addr] [Length $body_bytes_sent]
          [Gzip $gzip_ratio] [Sent-to $server] "$http_user_agent" "$http_referer"
standard: same, without the two upstream fields and without Sent-to
combined: stock nginx — default-host_access.log only
```

The parser dispatches on shape: a line starting with `[` is `proxy` or `standard`, told apart
by field count; anything else is `combined`. Unparseable lines land in a `_rejects` table with
their raw text, and a dbt test fails the build if the reject rate crosses a threshold. Silently
dropping lines you cannot parse is how a pipeline lies.

### Partitioning and retention

The daily partition is defined by **the timestamp inside each line**, never the filename. NPM
names files by proxy-host id and logrotate moves them weekly. A run for `dt=D` reads the live
logs plus every rotated sibling (`.1.gz` … `.4.gz`), parses `$time_local`, keeps that day.
Slower than trusting a filename, and the only version that survives a rotation landing mid-run.

**Weekly rotation keeping 4 → about five weeks, oldest line 27 July 2026.** Backfill cannot
reach further, ever. Once this runs, the lake becomes the only long-term history — which is a
real reason for the project to exist, and belongs in the README.

Landing: `s3://datalake/raw/npm_access/host=<fraserv|amb>/dt=YYYY-MM-DD/data.parquet`. One
object per host per day means one PUT, so the write is atomic without rename semantics S3 does
not have. Re-running overwrites it. That is the whole idempotency story for this source.

---

## 7. Source B — camera events

The most work, and the source that justifies the project. Everything else is tabular data being
moved; this one is untrusted model output being made trustworthy.

Keycloak and the middleware are in §5. The endpoints:

```
GET /api/export/events?from=<iso>&to=<iso>   attention events
GET /api/export/clips?from=<iso>&to=<iso>    clip metadata
GET /api/export/logs?from=<iso>&to=<iso>     event log entries
```

NDJSON, read-only. The contract is the part that matters: **`[from, to)`** half-open, so
adjacent partitions cannot double-count or drop a boundary row; **ordered by timestamp then
id**, giving deterministic output and keyset pagination for free; and the field list from §4 —
no media, no geometry, no identity.

### Late-arriving data — the honest bit

Vision analysis is asynchronous. An event lands with `confidence`, `summary` and `lifecycle`
empty and gets them minutes or hours later. Re-extracting yesterday tomorrow legitimately
returns *different* rows than it did today.

So "re-running a partition replaces it exactly" needs its precise form: **re-running replaces a
partition with the current truth for that date, and the marts always reflect the latest version
of each event.** Raw rows carry `_extracted_at`; the mart keeps the newest per `event_id`.
Nothing is silently overwritten and no analysis result is lost to a race.

This is the best interview answer in the repo. Write it down while it is fresh.

### Validating the model's output

`analysis-service.js` already does the hard part — fixed species vocabulary, unknown species
rewritten to `autre`, sightings under 0.6 confidence dropped, and a *missing* confidence
deliberately distinguished from a *low* one. The pipeline keeps that true, in dbt:
`accepted_values` on `species` against the same list, on `category` and `event_type`;
`confidence` null or within `[0, 1]`, because null is a real state and not a failure;
`not_null` on ids and `unique` on `event_id` in the mart.

If the model starts returning a species the app does not know, the build fails. That is
"tests that fail the pipeline, not just log", backed by a real case rather than a `not_null` on
a primary key.

---

## 8. Source C — Jellyfin, and the LiteLLM ledger

### Jellyfin

```
GET /Items?Recursive=true&IncludeItemTypes=Movie
    &Fields=Genres,ProviderIds,ProductionYear,CommunityRating,RunTimeTicks,DateCreated,MediaStreams
    &StartIndex=<n>&Limit=500
```

Field choices are in §4. ~2,400 items is a full snapshot per day in well under a second;
incremental sync would be premature, and snapshots are what SCD2 wants anyway.

`ProviderIds.Tmdb` comes back as a real id, so enrichment is `GET /movie/{tmdb_id}` — exact,
cheap, cacheable. The CSV had only a title, which means fuzzy matching, which means wrong
answers on remakes and translated titles. That difference alone justified reading the API.

**The model.** The one place a star schema is genuinely right rather than a demonstration:
`fct_library_title` (one row per movie per snapshot date), `dim_genre` plus
`bridge_title_genre` (genres are many-per-title; a bridge is the honest answer, and explaining
why beats flattening into `genre_1, genre_2, genre_3`), `dim_decade`, `dim_resolution_class`,
`dim_codec`. SCD2 over the daily snapshots gives library history — when a title arrived, when a
rating moved, when a file was re-encoded — free from snapshots you are taking anyway.

### LiteLLM spend

Straight `SELECT` from Postgres over `[from, to)` on `startTime`, into a daily partition. No
export to write, no new auth, no PII. 33,155 calls and €57.25 since 16 June.

It is here because of what it enables: cost per model per day, tokens in versus out, latency
distributions, failure rates, and spend attributed to team and tag — the `LiteLLM_Daily*Spend`
tables have that pre-aggregated. It is also the cleanest MCP demo in the project, because the
question and the deliverable are both obvious: *"what did I spend on Claude in August, by
model?"* → a formatted `.xlsx`.

---

## 9. Warehouse

DuckDB on a named Docker volume; dbt (`dbt-duckdb`) reads Parquet from MinIO through `httpfs`
and models `staging → intermediate → marts`. Rationale in §3.

The DuckDB file is a build artefact. Lose it and `dagster job execute` plus `dbt build` rebuilds
it from MinIO. Nothing in it is a source of truth — worth saying out loud, because it sets what
needs backing up (the bucket) and what does not (everything else).

Grafana already runs on `fraserv`, so the dashboard layer is a datasource away, and the geo
columns from §6 make a map panel a five-minute job. Evidence.dev can wait until there is
something worth screenshotting.

---

## 10. Orchestration

**Assets.** `DailyPartitionsDefinition` per source. `RetryPolicy(max_retries=3,
backoff=EXPONENTIAL)` on the ones crossing a network. NPM's reads a mounted file and should
fail loudly instead — a missing mount will not fix itself, and retrying hides it.

**Schedules.** One daily run at 03:00 for yesterday's partitions, after the rsync from
`amblevilleserv` lands.

**Sensors — two, doing different jobs, which is the point.** A polling sensor on the camera
export's newest event timestamp re-materialises today's partition when new events appear: the
intra-day path, and why late analysis results show up without waiting for 03:00. And a
`@run_failure_sensor` pushing to the notification channel `homelab_camera` already uses —
alerting on failure is on the checklist, and reusing an existing channel beats inventing one.

`PLAN.md` wanted a MinIO object sensor. Dropped: the camera S3 archive is off and
`camera-history` is the only bucket, so it would watch nothing. A polling sensor against a real
API is honest; a bucket sensor that never fires is decoration.

**Backfill.** Dagster's dialog, bounded by the five-week retention in §6. The checklist wants a
*documented case where it was used* — the first camera export schema change is the natural one.
Capture it when it happens rather than inventing one.

---

## 11. Privacy, in one place

| Field | Treatment | Why |
|---|---|---|
| Client IP | Geo/ASN derived at the boundary, then HMAC-SHA256 with a salt, truncated; raw discarded | Personal data under GDPR. Enrich-then-discard keeps every chart and none of the identifier. There is no raw landing zone, because that zone is the leak. |
| `client_ip_trust` | `direct` / `cdn_edge` / `restored` column | The column means different things before and after the §6 cutover. Mixing them produces confident wrong numbers. |
| Query strings, referer | Dropped | Carry tokens, reset links and session ids often enough not to gamble. |
| User-agent | Kept | Not identifying alone, and it is how bots are told from browsers. |
| `ai_platform` prompt/response | Never read | Personal content. The grant in §5 names tables and columns so it cannot widen by accident. |
| `Log.ip`, `.email`, `.username` | Hashed, same as above | Same data, different table. |
| Zone polygons | Never exported | A map of the property. Consistent with `homelab-camera`. |
| Camera `summary` | Warehouse only, never the seed | Describes people at the house. |
| Clip URLs, thumbnails | Never exported | Media stays on the server. |
| Jellyfin `Path`, `/Sessions` | Never requested | Topology and viewing habits. |
| Real data of any kind | Never published | `PLAN.md` §5: code, seed and screenshots. |

Rotating the IP salt breaks continuity in visitor counts. That is correct behaviour, and it
goes in the README.

---

## 12. CI and the synthetic seed

The seed generator is what makes this repo runnable by a stranger, so it is built in Stage 1,
not bolted on later. A script writing fake data in the same *shapes* as the real thing — all
three NPM formats, camera events with the real vocabulary and null patterns, movies in
Jellyfin's JSON shape, spend rows in LiteLLM's. Deterministic, committed as fixtures.

GitHub Actions on every push: generate seed → land it → `dbt build` → `dagster definitions
validate`. No secrets, no network, no server. Green on a fork means the repo is real.

Make the seed exercise the awkward cases: a Cloudflare address, an unparseable line, an IPv6
client, a null confidence, a species outside the vocabulary, a title with no TMDB id, a
zero-token failed LLM call. A seed where everything is clean tests nothing.

---

## 13. Build order

Each stage ends somewhere publishable. The failure mode is an ambitious repo that is 40% done
when someone asks for the link.

**Stage 0 — the real-IP snippet on both hosts (§6).** Half an hour, and every day it waits is a
day of `cdn_edge` rows. Do it before writing pipeline code.

**Stage 1 — NPM logs, both hosts, end to end.** Compose stack on `fraserv`, rsync from
`amblevilleserv`, three-format parser with a reject table, enrich-then-hash at the boundary,
`client_ip_trust`, MinIO landing, dbt staging and one mart, tests, seed generator, CI, README
with the Dagster UI screenshot and the 86% figure. **Stop and publish.**

**Stage 2a — LiteLLM spend.** Promoted ahead of Jellyfin: no export to build, no PII, and it
sets up the MCP demo. A day's work on top of Stage 1's scaffolding.

**Stage 2b — Jellyfin.** API key, snapshot asset, star schema, TMDB enrichment. Touches no
other repo.

**Stage 2c — camera.** Three systems, so in order, each verifiable alone: Keycloak client →
bearer middleware in `homelab_camera` → `/api/export/*` → Dagster asset → dbt vocabulary tests.
Needs the container healthy again first.

**Stage 3 — MCP server.** Unchanged from `PLAN.md` §7. Typed tools over the marts, read-only
DuckDB, row caps and timeouts, `.xlsx` output. Lead with the spend question.

---

## 14. Still open

1. **MaxMind GeoLite2 licence key** — free, needs an account, and the database file wants a
   monthly refresh. A small Dagster asset, or a cron and a note.
2. **MinIO disk headroom** on `homelab-camera-analysis_minio-data`. Parquet from ~200 MB/week
   of logs is small, but check rather than assume.
3. **Prometheus retention is the 15-day default** — no `--storage.tsdb.retention` flag is set.
   Confirm before relying on a rollup window wider than that.
4. **Whether `default-host` and `fallback` logs are worth ingesting at all.** They are scanner
   noise and TLS junk. Probably a separate, cheaper table — decide with the data in hand.
