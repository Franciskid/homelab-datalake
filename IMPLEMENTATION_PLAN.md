# Implementation plan

How `PLAN.md` actually gets built. That file is the *why*; this one is the *how*.

Written 2026-08-27 and corrected the same day against the real servers over SSH. Every number
and path below was read off the boxes. `PLAN.md` still governs the goals — §1 (why the repo
exists), §5 (published vs private), §6 (the checklist).

---

## 1. What checking the servers changed

[Private host inventory, source measurements, and access instructions omitted.]

---

## 2. The estate

[Private host inventory, source measurements, and access instructions omitted.]

---

## 3. Design choices, and how to defend them

Four decisions a technical interviewer will push on. Each is laid out the same way: **the
question that actually decides it**, the numbers for this project, **where the line sits**, the
strongest argument against, and what would change the answer.

The measurements everything below rests on:

| | Value | How it was measured |
|---|---|---|
| NPM logs, primary | [volume withheld]/day ([measurement withheld]) | [count withheld] B over 4 days |
| NPM logs, secondary | [volume withheld]/day | [count withheld] B over 5 days |
| **All sources, raw** | **~[volume withheld]/day ≈ [volume withheld]/year** | camera, Jellyfin and LiteLLM are rounding errors next to the logs |
| As Parquet (zstd + dictionary) | **~1–[volume withheld]/year** | 6–20× is normal for access logs: host, path and user-agent are highly repetitive, timestamps delta-encode |
| primary | 8 cores, [volume withheld] RAM ([volume withheld] free), [volume withheld] disk ([volume withheld] free) | `nproc`, `free`, `df` |

**Five years of this warehouse fits in the free RAM of the machine that produces it.** Nearly
every argument below is a consequence of that one line.

---

### 3.1 Why not Spark and HDFS

**The question that decides it:** Spark buys parallelism across machines and pays for it in
coordination. Does the workload need the parallelism enough to cover the coordination?

**The numbers.** A year is ~1–[volume withheld] of Parquet. The daily job processes ~[volume withheld]. Spark's fixed
costs — JVM startup, executor allocation, task serialisation, and shuffle whenever data crosses
a partition boundary — run from seconds locally to tens of seconds on a cluster, *before any
work happens*. A single-node engine reads the whole year in roughly the time Spark spends
deciding who will read it.

**Where the line sits.** Not at a magic byte count — at the point where one machine can no
longer finish the job inside its window. In practice that means:

- data materially larger than RAM *and* a query pattern that cannot stream through it,
- or a job long enough that a mid-run machine failure is likely, which is what Spark's lineage
  and task retry actually protect against,
- or a shared cluster where many teams need resource isolation.

None of the three apply. A useful published reference for the general point is *Scalability! But
at what COST?* (McSherry et al., HotOS 2015), which showed competent single-threaded
implementations beating distributed graph systems on real datasets — the paper's own framing is
that a system should have to justify the coordination it imposes. It is about graph workloads,
not a universal law, but the framing is the right one and citing it accurately is better than
citing it as gospel.

**HDFS deserves its own answer, because it is not the same argument.** HDFS exists to do three
things: store files bigger than one disk, replicate blocks for durability on unreliable
commodity hardware, and put compute next to data. On one host with one disk: the first is
irrelevant, the second is impossible — replication factor 1 is not durability, and the cluster
here reports 11 under-replicated blocks, so it is degraded on top — and the third is
meaningless. HDFS also stores its entire namespace in the NameNode's heap, which makes many
small files expensive; daily partitions are exactly many small files.

The deeper point, and the one worth leading with: **the industry decoupled storage from
compute, and HDFS is the coupled design.** EMR, Databricks, Snowflake and BigQuery all read
object storage. Choosing MinIO plus Parquet is choosing the architecture Spark itself now
targets, not choosing "simple over serious."

**The strongest argument against, and the answer.** *"Build for the scale you'll have, not the
scale you have."* The honest reply is that this trades a small one-time cost for a recurring
one. Premature distribution is paid every day — slower iteration, harder debugging, more to
operate. Migrating later is paid once, and **is already cheap because the storage format is
Spark's native format.** The expensive decision to reverse is the file format, and that one is
made in Spark's favour. Point Spark at the same bucket and nothing else changes.

That is the actual engineering claim: not *small data, simple tool*, but **I deferred the
reversible decision and pre-committed on the irreversible one.**

**What would change the answer.** A hundredfold volume increase — camera moving from event
metadata to per-frame embeddings, or logs going from [count withheld] to [count withheld] lines a day. A join whose
intermediate result exceeds one machine's memory. Or a genuine multi-tenant cluster with
isolation requirements.

---

### 3.2 Why daily batch and not streaming

**The question that decides it:** what decision gets made faster because the data arrived
faster? Freshness has value only where it changes an action.

**The numbers.** The questions this warehouse answers — traffic per service per month, garden
crossings per week, LLM spend in August, titles added this quarter — are all day-grain. Not one
of them changes if the data is four hours old rather than four seconds. Streaming would deliver
identical answers, later, after considerably more plumbing.

**The part that gets missed, and is worth leading with:** there *is* a real-time requirement in
this system — motion detected, notification pushed — and it is already met, in the camera app,
in-process, where the context lives. **The reactive path and the analytical path are different
systems with different guarantees.** Pushing alerting through a lake makes the lake a slow
alerting system and an unreliable warehouse at the same time. Keeping them separate is the
design decision; batch is just what falls out of it.

**What streaming actually costs**, named specifically, because "it's complex" is not an answer:

1. **Exactly-once is real work.** At-least-once delivery plus an idempotent sink, or a
   transactional sink. Dedup keys, state stores, TTLs on that state.
2. **Late data forces a watermark policy.** How long do you hold a window open, and what
   happens to a record arriving after it closed? This project has genuinely late data — vision
   analysis lands hours after the event (§7). In batch the answer is one line: re-run the day.
   In streaming it is a design.
3. **Reprocessing is the hard one.** Find a bug in a transform and batch re-runs the affected
   partitions. Streaming replays from the log — if retention still holds it — and every state
   store built from the old logic is now wrong. This is the problem Lambda architecture existed
   to work around and Kappa tried to solve, and neither is free.
4. **Operational surface.** A broker is a stateful distributed system to run, monitor, upgrade
   and rebalance. Three containers here, for two topics.
5. **Debuggability.** A failed batch partition is a re-runnable unit with a log. A failed stream
   is an offset plus in-flight state.

**This matters directly for the goal.** `PLAN.md` §6 asks the repo to demonstrate idempotent
loads, a working backfill path, and quality gates that fail the pipeline. All three are close to
free in batch and are each a project in streaming. Choosing batch is choosing to be able to
demonstrate the checklist rather than fight the substrate.

**The strongest argument against, and the answer.** *"Streaming isn't only about latency — it
smooths load and avoids the batch-window cascade where a late job delays everything after it."*
True, and a fair point at scale. At [volume withheld]/day the nightly job finishes in seconds, so there is
no window to overrun and no load to smooth.

**Do not claim the architecture is purely batch — it isn't, and an interviewer will spot it.**
The polling sensor in §10 re-materialises the current day when new camera events appear. That is
micro-batch at a fifteen-minute cadence. The design already sits on a spectrum, and the
defensible statement is *the freshness of each source is set by what its questions need*, not
*we chose batch*.

**On the Kafka pipeline already running here** — the sharp version, because the shallow version
is unfair to it. That pipeline is not proof streaming is wrong; it is a **sink mismatch**.
High-frequency host metrics were streamed into a relational log table. Measured, that table
costs **[measurement withheld]** ([volume withheld] / [count withheld] rows). Prometheus, running on the same host,
stores the same class of data as compressed time series — delta-of-delta timestamps, XOR-encoded
values — at one to two bytes per sample. The transport was fine; the storage model was off by
two orders of magnitude. The lesson to take is *match the storage model to the data's shape*,
which is also why §4 declines to ingest those nine million rows.

**What would change the answer.** A requirement to act within seconds on lake-derived state —
"alert when this specific vehicle appears" — or a volume where a daily batch stops fitting its
window. In either case the move is a streaming asset for that one source, not a rebuild:
Kafka is already there.

---

### 3.3 Why DuckDB

**The question that decides it:** what is the query shape, how many concurrent writers, and how
much operational surface is the answer worth?

**The query shape is OLAP** — scan a date range, filter, aggregate, group by a few dimensions.
Three properties of a columnar engine matter here, and being able to name the mechanism is what
separates a real answer from a preference:

- **Projection.** A `count(*) group by day, country` over a twenty-column table reads two
  columns. A row store reads all twenty, because the row is the unit of storage. That is close
  to an order of magnitude of I/O on this schema.
- **Encoding.** `host`, `user_agent` and `status` are low-cardinality and repeat constantly;
  dictionary encoding turns them into small integers. Timestamps delta-encode. This is where the
  6–20× Parquet compression comes from, and it is also why scans are fast — less to read.
- **Vectorised execution.** DuckDB processes batches of ~[count withheld] values per operator call rather
  than a row at a time, which keeps working sets in CPU cache and lets the compiler use SIMD.
  Row-at-a-time interpretation is the thing it is avoiding.

**Plus Parquet pushdown:** row groups carry min/max statistics per column chunk, so a
`WHERE dt BETWEEN` prunes whole row groups before any bytes are read. Partitioning by date makes
that pruning near-total.

**Why not Postgres, which is already running.** Two reasons, technical first. It is row-oriented,
so the projection argument above works against it on every analytical query, and it has no
dictionary encoding for the repetitive string columns that dominate this schema. Second, and
operationally: that instance is the shared database behind Keycloak, the AI platform and
banking. An analytical scan there competes for buffer cache and connections with the front door
lock. Blast radius is a real consideration, but it is the second argument, not the first.

**Why not ClickHouse.** It is the right answer at high read concurrency or two orders more data,
and it is a server with its own operational story — replication, merges, schema management. At
this size it is a system to run for benefits nobody is claiming.

**The limitations, volunteered before being asked** — this is the part that makes the answer
credible:

- **One writer process at a time.** Concurrent readers are fine, and multiple threads inside one
  process are fine, but two Dagster processes cannot write the same file. Serialising a nightly
  batch is free; under streaming ingest this would be disqualifying — which is one more reason
  §3.2 lands where it does.
- **No HA, no replication.** It is a file. The answer here is that it is a *build artefact*:
  `dagster job execute` plus `dbt build` reconstructs it from MinIO, so the bucket is what gets
  backed up.
- **Not a serving layer for many concurrent users.** A team dashboard needs a server in front,
  or precomputed extracts.
- **Large joins can spill.** It handles it, but "fits in memory" remains the happy path.

**What would change the answer.** Concurrent writers, dozens of simultaneous dashboard users,
a need for row-level updates at scale, or crossing roughly a terabyte.

---

### 3.4 Why Dagster

**The question that decides it:** what is the unit you reason about — *a task that ran*, or *a
table that is up to date*?

Airflow's primitive is a task execution keyed by a logical date. Dagster's is an **asset**: a
named piece of data, its dependencies, and the history of which partitions have been
materialised. That difference is not cosmetic here, for one specific reason.

**The NPM input files do not map to days** (§6). Rotation is weekly, one file spans several
dates, and a day's lines can live in the live log or in any of four gzipped siblings. So there
is no filesystem fact that answers "is 14 August done?". **Something has to be the record of
truth about partition state, and the choice is between the orchestrator and a table you write
and maintain yourself.** Dagster models `(asset, partition)` as a first-class entity with
materialisation history, so the answer is a query against the system rather than bookkeeping you
have to keep correct. That is the concrete reason, and it is worth leading with over anything
about declarative programming.

The rest follows from the same model:

- **Backfill is a range selection**, with per-partition runs and concurrency limits, not a script
  with a date loop.
- **Lineage is declared, not drawn.** Assets state their inputs and the graph is derived, so it
  cannot silently disagree with what the code actually reads. In Airflow the DAG edges and the
  real data dependencies are two separate things that drift.
- **Asset checks are first-class**, which is how `PLAN.md` §6's "tests that fail the pipeline"
  gets enforced at the orchestration layer and not only inside dbt.
- **I/O managers separate *what is computed* from *where it lands*.** The same asset code writes
  to a local directory in CI and to MinIO in production — that is precisely what makes the CI
  story in §12 work without a second implementation.
- **`dagster dev` runs the real definitions locally**, against the seed.
- **The UI is the ingestion IHM** that `PLAN.md` §4 decided not to build.

[Private career context or unrelated service inventory omitted.]

**The costs, volunteered.** Smaller ecosystem and fewer integrations than Airflow. The asset
abstraction takes a day to click if you have only used task schedulers. And Dagster's own
footprint — daemon, webserver, metadata database — is not nothing on a homelab box already
running 54 containers.

**What would change the answer.** A team already standardised on something else. Or a workload
that genuinely is "run this script at 3am" with no partitioning, no backfill and no lineage — at
which point cron is the honest answer and Dagster is overhead.

---

### 3.5 The short versions

For when there are thirty seconds, not five minutes.

[Private career context or unrelated service inventory omitted.]

[Private host inventory, source measurements, and access instructions omitted.]

---

## 5. How to connect, verify and modify each project

[Private host inventory, source measurements, and access instructions omitted.]

---

## 6. Source A — NPM logs, and getting real visitor IPs

### Restoring the real IP — done, 27 Aug 2026

**The ranges were never the problem. One header name was.**

The plan here originally said to write Cloudflare's address ranges into a
`set_real_ip_from` list. That turned out to be unnecessary: NPM maintains its own trust list at
`conf.d/include/ip_ranges.conf` — 265 entries, covering **all 21 ranges Cloudflare currently
publishes**. It was already trusting Cloudflare completely.

What it does with that trust is read `real_ip_header X-Real-IP`. **Cloudflare does not send
`X-Real-IP`; it sends `CF-Connecting-IP`.** So nginx trusted the connection, looked for a header
that was never there, and left `$remote_addr` as the edge address. Six weeks of logs recorded
Cloudflare instead of visitors because of one header name.

The fix is one directive. It cannot go in the http context — NPM sets `real_ip_header` at
`nginx.conf:69` and nginx rejects a duplicate in the same context, which is exactly how the
first attempt failed. It goes in the **server** context instead, via
`/data/nginx/custom/server_proxy.conf`, a path NPM includes in every proxy-host server block:

```nginx
real_ip_header CF-Connecting-IP;
```

Deployed file and instructions: [`infra/npm/`](infra/npm/). Deploy is `nginx -t` gated —
a bad file here takes every service offline.

**Result, measured across all 44 proxy hosts on 27 August:**

| | Requests | Cloudflare edges |
|---|---|---|
| Before 21:27 UTC | [withheld] | [count withheld] (**[rate withheld]**) |
| After 21:27 UTC | [withheld] | 0 (**[rate withheld]**) |

`scripts/verify-real-ip.sh` reprints that by hour on demand.

**Why this is safe, and worth being able to explain.** nginx only honours the header when the
connecting socket is itself in a trusted range. A client connecting directly cannot forge an
address, because its own address is not in Cloudflare's ranges. A request arriving without the
header is left alone. The failure mode is falling back to the edge address — losing precision,
never trusting a lie.

The residual risk is narrow and real: something originating *from* Cloudflare's network, such as
a Worker, connects from a trusted range and could set the header freely. Closing that means
restricting ports 80/443 at the firewall to Cloudflare ranges so nothing else can reach the
origin at all. Worth doing, not done — it can lock you out if a range list goes stale, so it
needs more care than a config reload.

**Not deployed to secondary, deliberately.** That host takes traffic directly: 8 of [count withheld]
logged addresses were Cloudflare. `$remote_addr` there is already the visitor. Trusting
`CF-Connecting-IP` would add a spoofable header for no benefit.

**Maintenance is a check, not a deploy.** NPM regenerates its range list on container start, so
`scripts/check-cloudflare-ranges.sh` compares it against Cloudflare's published list and says
whether a restart is needed. Today: 22 published, 0 missing. A range added after NPM's last
restart is simply not trusted yet — which fails safe.

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

Landing: `s3://datalake/raw/npm_access/host=<primary|secondary>/dt=YYYY-MM-DD/data.parquet`. One
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

Field choices are in §4. ~[count withheld] items is a full snapshot per day in well under a second;
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
export to write, no new auth, no PII. [count withheld] calls and [spend withheld] since 16 June.

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

Grafana already runs on `primary`, so the dashboard layer is a datasource away, and the geo
columns from §6 make a map panel a five-minute job. Evidence.dev can wait until there is
something worth screenshotting.

---

## 10. Orchestration

**Assets.** `DailyPartitionsDefinition` per source. `RetryPolicy(max_retries=3,
backoff=EXPONENTIAL)` on the ones crossing a network. NPM's reads a mounted file and should
fail loudly instead — a missing mount will not fix itself, and retrying hides it.

**Schedules.** One daily run at 03:00 for yesterday's partitions, after the rsync from
`secondary` lands.

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

Each stage ends somewhere publishable. The failure mode is an ambitious repo that is [rate withheld] done
when someone asks for the link.

**Stage 0 — real-IP restoration. Done 27 Aug 2026 (§6).** One directive on primary; not needed
on secondary. [rate withheld] edge addresses before, [rate withheld] after.

**Stage 1 — NPM logs, both hosts, end to end. Built 28 Aug 2026.** Three-format parser
(validated on [count withheld] real lines, zero rejects), enrich-then-hash boundary, `client_trust`,
multi-partitioned assets, MinIO landing, DuckDB, dbt through marts, 50 pytest cases and 30
dbt nodes green, seed generator, CI, compose stack, rsync script, README.
**Deployed 28 Aug 2026** to `primary`, serving https://data.example.test behind Keycloak
(client `datalake-web`, realm role `datalake-user`, oauth2-proxy with PKCE). First real
run: [count withheld] rows over two days, **zero rejects**, [volume withheld] of Parquet, dbt 30/30 green.
Schedule enabled at 03:00 UTC.

Outstanding: the Dagster UI screenshot, and the secondary log sync — `primary`
cannot reach that host (no route, no key), so cross-site SSH needs setting up before
`scripts/sync-secondary-logs.sh` can run. Until then the `secondary` partition fails loudly rather
than writing empty days.

**Stage 2a — LiteLLM spend. Built and deployed 28 Aug 2026.** [count withheld] calls and [spend withheld]
backfilled over 73 daily partitions, 25 models, dbt 54/54 green. Read-only Postgres role
scoped to the single table; prompts excluded by name; API key, caller address and end
user hashed at the boundary. Schedule enabled at 03:15 UTC.

Two bugs the tests caught: `is_self_hosted` came back with three values because a null
`provider` propagated through an `OR` (`null = 'ollama'` is null, not false), and empty
source tables were being created as placeholders that dbt could not compile against --
they now carry their declared schema, which is also what keeps CI green with no
database.

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
2. **MinIO disk headroom** on `homelab-camera-analysis_minio-data`. Parquet from ~[volume withheld]/week
   of logs is small, but check rather than assume.
3. **Prometheus retention is the 15-day default** — no `--storage.tsdb.retention` flag is set.
   Confirm before relying on a rollup window wider than that.
4. **Whether `default-host` and `fallback` logs are worth ingesting at all.** They are scanner
   noise and TLS junk. Probably a separate, cheaper table — decide with the data in hand.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
