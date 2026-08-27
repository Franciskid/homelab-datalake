# Implementation plan

How `PLAN.md` actually gets built. That file is the *why*; this one is the *how*, decided
on 2026-08-27 after checking what the three source platforms really expose.

Read `PLAN.md` first. Nothing here overrides its goals — §1 (why the repo exists), §5
(published vs private) and §6 (the checklist) still govern.

---

## 1. What changed after looking at the sources

Four things in `PLAN.md` were written from memory. Three needed refining, one was wrong.

**"Nginx, Cloudflare, host and container metrics" → Nginx Proxy Manager access logs, only.**
One log source, done properly, beats four done shallowly. NPM is the front door for every
service on the box, so its access log already contains the interesting cross-service traffic.
Cloudflare and container metrics can come later; they add volume, not new problems to solve.

**"Jellyfin — the CSV is already exported" → pull from the Jellyfin API.**
A CSV someone clicked once is a file, not a pipeline. It has [count withheld] rows, French dates,
semicolon separators, and `1440 * 1072` as a resolution string — all artefacts of the export
button. The API gives typed fields and, crucially, `ProviderIds.Tmdb`, which turns TMDB
enrichment from fuzzy title matching into a key lookup. Keep the CSV as the synthetic seed's
starting shape; stop treating it as the source.

**"Camera metadata JSON already goes to MinIO" → it doesn't.**
`ARCHIVE_S3_ENABLED=false`, and even switched on, the archive uploader ships *clips*, not the
event store. The real data lives in `data/attention-events.json`, `data/history.json` and
`data/event-logs.json` on the camera container's volume. So the export has to be built.

**Camera export goes through Keycloak, not a static token.**
Decided today. `homelab_camera` authenticates humans through `auth.example.test/realms/homelab`
already; a machine reader should live in the same place, not in a second `.env` with a shared
secret nobody can rotate or revoke. One identity provider, one place to look when asking who
can read what. Costs more up-front — see §4.

---

## 2. Where it runs

Docker Compose on the server, alongside the sources.

Everything the pipeline reads is already on that box: the NPM log volume, the camera
container, Jellyfin, MinIO. Running there means no tunnels, no new public endpoints, and
nothing exposed that isn't already. The laptop edits code and pushes; it doesn't run the
pipeline against real data.

The trade: `docker compose up` on a fresh clone can't reach any of it. Handled in §9 — CI and
local dev run against a synthetic seed, which is what makes the repo reproducible for someone
who isn't you.

New bucket `datalake` on the existing MinIO at `s3.homelab.example.test`. A second MinIO would
be tidier to publish and pointless to run.

---

## 3. Source A — Nginx Proxy Manager logs

### Getting them out

NPM writes `/data/logs/proxy-host-<id>_access.log` (plus `fallback_access.log` and the
matching `_error.log`) inside its container. Mount that directory **read-only** into Dagster.
Read-only matters: a pipeline that can corrupt the reverse proxy's logs is a pipeline that
can take the house offline.

### The partition problem, and why it's the interesting part

NPM's filenames carry a proxy-host id, not a date. Rotation is logrotate's, so yesterday's
lines can sit in `.log`, `.log.1`, or a `.gz` depending on when the pipeline runs.

So the daily partition is defined by **the timestamp inside each line**, never by the file it
came from. The asset for `dt=2026-08-27` reads the live log plus every rotated sibling, parses
each line's `$time_local`, and keeps only that day. Slower than trusting a filename, and the
only version that survives a rotation happening mid-run.

This is also what makes re-running a partition safe: same date filter, same input lines, same
output. Rotation can't change the answer, only where the lines were found.

### PII at the boundary

The client IP never reaches storage in the clear. Parse → hash → write. There is no
"raw landing zone with real IPs that we clean up later", because that zone is the leak.

Two columns replace `remote_addr`:

- `client_hash` — HMAC-SHA256 of the IP with a salt from the environment, truncated to 16
  bytes. Lets you count distinct visitors and spot a scanner hammering one path. Not
  reversible without the salt, and the salt is never committed.
- `client_net` — IPv4 truncated to /24, IPv6 to /48. Coarse enough not to identify a
  household, precise enough to tell a datacentre range from a home ISP.

Query strings are dropped; paths are kept. Query strings carry tokens, reset links and
session ids often enough that keeping them is a bet with no upside. Referer is dropped for
the same reason. User-agent stays — it's how you separate bots from browsers.

Rotating the salt breaks continuity in the visitor counts. That's the correct behaviour and
belongs in the README.

### Landing shape

`s3://datalake/raw/npm_access/dt=YYYY-MM-DD/data.parquet` — one object per partition.

One object, one PUT, and the write is atomic without needing rename semantics S3 doesn't
have. Re-running the partition overwrites it. That's the whole idempotency story for this
source, and it fits in a sentence.

---

## 4. Source B — camera events

The most work, and the source that justifies the project. Everything else is tabular data
being moved; this one is untrusted model output being made trustworthy.

### Keycloak first

In the `homelab` realm:

1. New client `datalake-exporter` — confidential, **service accounts on**, standard flow
   **off**. It is not a login; it is a program.
2. Client role `export:read`, assigned to that client's service account.
3. Dagster holds `client_id` + `client_secret`, does `client_credentials`, caches the access
   token until shortly before expiry.

Revoking the pipeline's access is then one toggle in Keycloak, next to every human account.
That's the whole reason for the extra step.

### Then the camera app has to accept a bearer token

It currently doesn't. `auth-service.js` does the authorization-code dance and keeps a session
cookie. It reads JWT claims but never verifies a signature — fine when the token came
straight from Keycloak over TLS, **not** fine for a token handed to it by a caller.

So, in `homelab_camera`:

- Add `jose` (the app has no JWT library today).
- A bearer middleware: fetch JWKS from the realm, cache it, verify signature, `iss`, `exp`,
  and require the `export:read` role. Reject everything else.
- Mount it on `/api/export/*` **only**. The existing session auth keeps guarding the rest.

Signature verification is not optional here and is worth stating plainly in that repo's
commit: without it, anyone who can reach the endpoint writes their own token.

### The endpoints

Three, all NDJSON, all read-only:

```
GET /api/export/events?from=<iso>&to=<iso>   attention events
GET /api/export/clips?from=<iso>&to=<iso>    clip metadata (history + archive)
GET /api/export/logs?from=<iso>&to=<iso>     event log entries
```

The contract, which is the part that matters:

- **`[from, to)`** — half-open. Adjacent partitions can never double-count or drop a row on
  the boundary.
- **Ordered by timestamp, then id.** Deterministic output, and keyset pagination for free.
- **No media.** No playlist URLs, no thumbnails, no clip paths.
- **No zone geometry.** `zoneId` and `zoneName` go; the polygon points stay home. Those
  coordinates are a map of the property, and `homelab-camera` already argues against handing
  that over. Consistency here is not fussiness — an interviewer who spots the contradiction
  reads it as not having thought about it.
- **No user identity.** `createdBy` collapses to a stable hash. Analytics needs "was this
  manual or automatic", not who.

### Late-arriving data — the honest bit

Vision analysis is asynchronous. An event lands with `confidence`, `summary` and
`lifecycle` empty, and gets them minutes or hours later. Re-extracting yesterday tomorrow
legitimately returns *different* rows than it did today.

So "re-running a partition replaces it exactly" needs its precise form: **re-running a
partition replaces it with the current truth for that date, and the marts always reflect the
latest version of each event.** Raw rows carry `_extracted_at`; the mart keeps the newest row
per `event_id`. Nothing is silently overwritten, and no analysis result is lost to a race.

This is the best interview answer in the whole repo. Write it down while it's fresh.

### Validating the model's output

`analysis-service.js` already does the hard part — fixed species vocabulary, unknown species
rewritten to `autre`, sightings under 0.6 confidence dropped, a *missing* confidence
deliberately distinguished from a *low* one.

The pipeline's job is to make sure that stays true, in dbt:

- `accepted_values` on `species` against the same vocabulary list.
- `accepted_values` on `category` and `event_type`.
- `confidence` is null or within `[0, 1]` — null is a real state, not a failure.
- `not_null` on ids, `unique` on `event_id` in the mart.

If the model starts returning a species the app doesn't know, the build fails. That's the
"tests that fail the pipeline, not just log" line on the checklist, backed by a real case
instead of a `not_null` on a primary key.

### Landing shape

`s3://datalake/raw/camera_events/dt=YYYY-MM-DD/data.parquet`, same one-object rule.

---

## 5. Source C — Jellyfin

### Getting it out

API key in Jellyfin's dashboard, sent as `Authorization: MediaBrowser Token="..."`.

```
GET /Items?Recursive=true&IncludeItemTypes=Movie
    &Fields=Genres,ProviderIds,ProductionYear,CommunityRating,RunTimeTicks,DateCreated,MediaStreams
    &StartIndex=<n>&Limit=500
```

Metadata only. No `/Sessions`, no `/Users`, no playback history — `PLAN.md` §2 already settled
that, and the API makes it a one-line decision to keep. `Path` is dropped too: filesystem
layout is topology, not analysis.

~[count withheld] items is a full snapshot per day, well under a second. Incremental sync would be
premature optimisation on a dataset this size, and snapshots are what SCD2 wants anyway.

### Why the API beats the CSV, concretely

`ProviderIds.Tmdb` comes back as a real id. TMDB enrichment becomes
`GET /movie/{tmdb_id}` — exact, cheap, cacheable. The CSV has only a title, which means fuzzy
matching, which means wrong answers on remakes and translated titles. That difference alone
justified the switch.

### The dimensional model

The one place a star schema is genuinely the right shape rather than a demonstration.

- `fct_library_title` — one row per movie per snapshot date
- `dim_genre` + `bridge_title_genre` — genres are many-per-title; a bridge is the honest
  answer, and explaining why beats flattening into `genre_1, genre_2, genre_3`
- `dim_decade`, `dim_resolution_class`, `dim_codec`

SCD2 over the daily snapshots gives library history — when a title arrived, when a rating
moved, when a file was re-encoded. Free from snapshots you're taking anyway.

---

## 6. Warehouse

DuckDB file on a named Docker volume. dbt (`dbt-duckdb`) reads the Parquet in MinIO through
DuckDB's httpfs, models into `staging → intermediate → marts`.

DuckDB rather than Postgres because the whole warehouse is one file that can be copied,
inspected and thrown away, and because it reads Parquet from S3 natively. At this data volume
a server-based warehouse is a container to maintain with no benefit.

The DuckDB file is a build artefact. If it's lost, `dagster job execute` + `dbt build`
rebuilds it from MinIO. Nothing in it is a source of truth. Worth saying out loud, because it
sets what needs backing up (the bucket) and what doesn't (everything else).

---

## 7. Orchestration

**Assets.** `DailyPartitionsDefinition` per source, `RetryPolicy(max_retries=3,
backoff=EXPONENTIAL)` on the two that cross a network. NPM's reads a mounted file and should
fail loudly rather than retry — a missing mount won't fix itself.

**Schedules.** One daily run at 03:00 for yesterday's partition, after logrotate has moved.

**Sensors — two, doing different jobs, which is the point.**

1. A polling sensor on the camera export's newest event timestamp. New events → re-materialise
   today's partition. This is the intra-day path, and it's why late-arriving analysis results
   show up without waiting for tomorrow's 03:00.
2. A `@run_failure_sensor` that pushes to the notification channel `homelab_camera` already
   uses. Alerting on failure is on the checklist, and reusing an existing channel is a better
   story than a new one.

`PLAN.md` wanted a MinIO object sensor. Dropped: the camera S3 archive is off, so a sensor
watching that bucket would be watching nothing. A polling sensor against a real API is honest;
a bucket sensor that never fires is decoration. If archiving is switched on later, add it then.

**Backfill.** Dagster's backfill dialog covers it. The checklist asks for a *documented case
where it was used* — the first schema change to the camera export is the natural one. Capture
it when it happens rather than inventing one.

---

## 8. Privacy, in one place

| Field | Treatment | Why |
|---|---|---|
| Client IP | HMAC hash + /24 or /48 net | GDPR personal data. Hashed before write, never stored raw. |
| Query strings, Referer | Dropped | Carry tokens and session ids often enough not to gamble. |
| Zone polygons | Never exported | A map of the property. Consistent with `homelab-camera`. |
| Clip URLs, thumbnails | Never exported | Media stays on the server. |
| `createdBy` | Hashed to a stable id | Analytics needs manual-vs-automatic, not a name. |
| Jellyfin `Path`, `/Sessions` | Never requested | Topology and viewing habits. Neither adds anything. |
| Real data of any kind | Never published | `PLAN.md` §5. Repo ships code, seed and screenshots. |

---

## 9. CI and the synthetic seed

The seed generator is what makes this repo runnable by a stranger, so it gets built in
Stage 1, not bolted on later.

A script writing three fake datasets with the same *shapes* as the real ones — NPM log lines
in NPM's format, camera events with the real vocabulary and the real null patterns, movies in
Jellyfin's JSON shape. Deterministic seed, committed as fixtures.

GitHub Actions on every push: generate seed → land it → `dbt build` → `dagster definitions
validate`. No secrets, no network, no server. If it goes green on a fork, the repo is real.

Make the seed exercise the awkward cases — a null confidence, a species outside the
vocabulary, an IPv6 client, a title with no TMDB id. A seed where everything is clean tests
nothing.

---

## 10. Build order

Each stage ends somewhere publishable. The failure mode is an ambitious repo that's [rate withheld] done
when someone asks for the link.

**Stage 1 — NPM logs end to end.** Compose stack, one partitioned asset, the parse-and-hash
boundary, MinIO landing, dbt staging + one mart, tests, seed generator, CI, README with the
Dagster UI screenshot. No camera, no Jellyfin, no Keycloak. **Stop and publish.**

**Stage 2a — Jellyfin.** Pure addition: API key, snapshot asset, star schema. No changes to
other repos, so it lands fast and proves the platform takes a second source cleanly.

**Stage 2b — camera.** The one that touches three systems. In order, each verifiable alone:
Keycloak client → bearer middleware in `homelab_camera` (its own PR, its own tests) →
`/api/export/*` → Dagster asset → dbt models and vocabulary tests.

**Stage 3 — MCP server.** Unchanged from `PLAN.md` §7. Typed tools over the marts, read-only
DuckDB, row caps and timeouts, `.xlsx` output. The headline, and the demo video.

---

## 11. To verify on the server before Stage 1

Four things assumed here that a terminal on the box would settle in a minute:

1. **NPM's exact log format** — `docker exec <npm> cat /etc/nginx/conf.d/include/log.conf`,
   and a couple of real lines. The parser is written against whatever that says.
2. **Rotation policy** — how many days NPM keeps, and whether rotated files are gzipped.
   Sets how far back a backfill can reach, which belongs in the README.
3. **The log volume's host path** — for the read-only mount.
4. **MinIO credentials** with write access scoped to a new `datalake` bucket. A separate key
   from the camera archive's; one key per consumer.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
