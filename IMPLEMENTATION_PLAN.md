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

### Take part of it

| Source | Take | Leave |
|---|---|---|
| **`ai_platform.usage_events`** ([count withheld] rows, [volume withheld] at first survey; [count withheld] rows, [volume withheld] by 2026-09-14) | Everything — see the note below the table. | Nothing. Superseded 2026-09-14; §15 has the reversal and why. |
| **`primary_homelab.Log`** ([count withheld] rows, [volume withheld]) | The 0.3% that is actually events: [count withheld] `HTTP_METRIC`, [count withheld] `DB_METRIC`, [count withheld] `SOCKET_EVENT`, [count withheld] `CV_EVENT`, 164 `AUTH_EVENT` | [count withheld] `SYSTEM_METRIC` rows. They are a worse copy of Prometheus, sampled by an app instead of a purpose-built scraper. Carries `ip`, `email`, `username` — §11 applies. |
| **Prometheus** | Daily rollups per instance and metric | The raw 15-second series. |
| **status-page `incidents.json`** | Incidents and uptime samples ([volume withheld], 90-day retention) | Nothing much — it is small and high-signal. |

Two of these are worth the paragraph they will get in an interview. `usage_events` **was**
select the columns, leave the payload — the same table is [volume withheld] or [volume withheld] depending on whether
you thought about it — until the owner reversed that decision on 2026-09-14 for the whole
`ai_platform` database, not just this one table. §15 is that reversal, in full, and why the
argument below is not wrong so much as answering a question the owner no longer asked: it was
never "is the payload useless", it was "is this repo a public portfolio piece or a private
warehouse", and once the repo held camera footage descriptions and reverse-proxy visitor
traffic in a lake nobody but the owner reads, treating the owner's own conversations
differently had stopped being a privacy boundary and started being an inconsistency. And
Prometheus keeps **15 days by default**; rolling it into the lake turns fifteen days into
forever, which is a real reason for a warehouse to exist rather than a decorative one.
status-page joins to NPM logs on time and answers "did traffic drop because nobody came, or
because it was down?"

### Leave it

- **`Log`'s `SYSTEM_METRIC` rows** — [count withheld] rows of host metrics in a relational table at a measured
  350 bytes each, where Prometheus stores the same shape at one to two bytes per sample (§3.2).
  The single biggest table on the box, and the correct amount to ingest is zero.
- **`ai_platform` prompts and responses, `banking_tracker`, Nextcloud** — personal content and
  financial records. Not a close call.
- **wg-easy** — VPN peers are who connects from where. Network topology.
- **Authelia / Keycloak auth logs** — login patterns are genuinely interesting and it is a
  credential-adjacent audit trail. Bad risk-to-reward for a repo whose point is being shown to
  strangers.
- **qbittorrent, jackett, radarr, sonarr** — download history. Awkward to publish anything
  derived from, aggregates included. Not worth the paragraph.
- **Jenkins** ([count withheld] jobs, [volume withheld]), **syncthing, portainer, ombi, jellyseerr** — real but thin, or
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
| `ai_platform` prompt/response, message content | Read and landed in full | **Reversed 2026-09-14** — was "never read". §15 has the decision and the (real) enforcement that replaces it: named columns still, no `select *`, but the two credential/PII exclusion lists below are unmoved and non-negotiable. |
| `ai_platform` credentials — key hashes/ciphertext, provider credentials, push-subscription keys, the ntfy topic, per-user tool config values, an MCP server's command/metadata | Never read | Not content, and the 2026-09-14 reversal is explicitly about content. A working credential in a readable table is a different kind of problem from data being available; see the registry in §15. |
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

Only `primary` is ingested. The other host's logs are **out of scope by decision, not
by accident**: joining the two sites at the host level would mean a permanent trust
relationship between them, and the rule here is that they talk over public URLs with
tokens or not at all. `NPM_HOSTS` names what is mounted, and an unmounted host fails
loudly rather than writing empty days that would read as quiet ones.

**Stage 2a — LiteLLM spend. Built and deployed 28 Aug 2026.** [count withheld] calls and [spend withheld]
backfilled over 73 daily partitions, 25 models, dbt 54/54 green. Read-only Postgres role
scoped to the single table; prompts excluded by name; API key, caller address and end
user hashed at the boundary. Schedule enabled at 03:15 UTC.

Two bugs the tests caught: `is_self_hosted` came back with three values because a null
`provider` propagated through an `OR` (`null = 'ollama'` is null, not false), and empty
source tables were being created as placeholders that dbt could not compile against --
they now carry their declared schema, which is also what keeps CI green with no
database.

**Stage 2b — Jellyfin. Built and deployed 28 Aug 2026.** [count withheld] titles, [rate withheld] with a TMDB
id, star schema with a genre bridge and SCD2, dbt 86/86 green. Schedule enabled at 03:30
UTC.

TMDB enrichment was built and then cut down. Most of what it fetched, Jellyfin already
had: it queries TMDB itself during a scan, so `CommunityRating` **is** TMDB's vote
average, and the original title, production country and release date come back at
[rate withheld], [rate withheld] and [rate withheld]. Fetching those again put an API key between the warehouse and
data already on disk, and left the columns null until somebody made an account. Those
four now come from the library snapshot. TMDB keeps only what Jellyfin has no field
for -- budget, revenue, popularity, vote count, original language -- and stays optional
in the way geo is, so CI never needs a credential.

**Stage 2c — camera. Built and deployed 29 Aug 2026.** Keycloak client → bearer middleware
in `homelab_camera` → `/api/export/*` → Dagster asset → dbt vocabulary tests. [count withheld] events,
[count withheld] clips and [count withheld] logs over 23 days.

It broke once, on 30 Aug 2026, and the way it broke is the lesson. A deploy of the camera
app overwrote `src/create-app.js` and `package.json`; the export route modules survived,
but the four lines that mounted them and the `jose` dependency did not. The endpoints
answered 404, and the nightly ingest failed with them. Nothing could be restored: that
repository had no commits, and no image older than the deploy still carried the code. It
had to be written again from the modules' own interface.

Two things follow. The camera repository must be committed -- an uncommitted feature is
one deploy away from gone. And the two losses came from one event but surfaced one at a
time: while the routes were unmounted, `export-auth.js` was never loaded, so its missing
dependency could not raise. Repairing the first fault is what revealed the second.

**Stage 3 — MCP server. Built and deployed 29 Aug 2026.** Live at
`https://data.example.test/mcp`, behind a Keycloak bearer token.

Four dataset tools plus discovery and export, over a semantic layer of dimensions and
measures. No tool takes SQL: a question is a typed structure whose every name is looked
up in a registry and whose every value is a bound parameter, so there is no path from
model output to query text. Bounded by a row cap applied as `LIMIT`, a date-range cap,
and a wall-clock deadline enforced by a watchdog calling `interrupt()` -- DuckDB has no
statement timeout, so without that the deadline would be a comment.

Two things this stage forced that were not in the plan:

**The marts were not on a schedule.** The sources refreshed nightly; the marts refreshed
whenever somebody ran `dbt build` by hand. A serving layer over stale marts is worse than
none, so dbt became `@dbt_assets` -- one Dagster asset per model rather than one opaque
asset shelling out, because a single green dot for twenty models is what cron already
gives you. The lineage now runs unbroken from a Parquet partition to a mart, and
`analytics_daily` at 04:15 rebuilds the whole chain.

**DuckDB allows one writing process, and a read-only open fails under that lock** rather
than blocking -- so a reader pointed at the warehouse would have raised for the length of
every nightly build. Hence a serving copy, holding only the marts, swapped in atomically
after a successful build. It is opened by `ATTACH` and not `duckdb.connect(path)`,
because DuckDB caches database instances by path within a process: after the swap a
"fresh" connect keeps returning the previous build's numbers while any other handle is
open, and nothing raises. Measured at [measurement withheld] stale rows against the corrected result.

Verified in production: 107/107 dbt tests, 10 marts and [count withheld] rows published, and from
the public internet no token / garbage token / forged-but-well-formed token all rejected
with 401.

**Done 30 Aug 2026.** The Keycloak client `datalake-mcp` exists, its service account holds
the `datalake:query` client role, and the endpoint has been exercised end to end from the
public internet with a real token: seven tools listed, real figures returned, and a
formatted workbook written.

It is also registered as an upstream on the LiteLLM MCP gateway, which mints its own
short-lived token by `client_credentials` -- no static bearer, and no refresher sidecar.
Three separate things had to be true, and each fails silently on its own: the entry in
`litellm/config.yaml` needs `oauth2_flow: client_credentials` (without it LiteLLM treats
the server as interactive per-user OAuth and never mints anything) and an explicit
`token_url` (it does not read the server's own discovery document, even though the
datalake publishes one); the seven tools need adding to the runtime's deny-by-default
profile, or an agent cannot even discover them; and `datalake` needs listing in
`MCP_SERVER_ALIASES`, or the API does not look for them.

---

## 14. Still open

1. ~~**MaxMind GeoLite2 licence key.**~~ **Done 30 Aug 2026.** `scripts/refresh-geoip.sh`
   downloads both databases into the `geoip` volume through a short-lived container that
   has it read-write, writing under a temporary name and moving into place, so a reader
   never sees a half-written database. Geography is [rate withheld] populated across [count withheld]
   requests. The script prints the monthly cron line; MaxMind rebuilds twice a week.

   The ordering constraint is the part worth remembering: geography is derived while the
   address is still in hand and the address is discarded in the same step. So a partition
   built before the databases existed cannot gain geography from a mart rebuild -- its log
   lines have to be parsed again. That is why enabling geo meant rebuilding all 34 days
   rather than only the missing ones.
2. **MinIO disk headroom** on `homelab-camera-analysis_minio-data`. Parquet from ~[volume withheld]/week
   of logs is small, but check rather than assume.
3. **Prometheus retention is the 15-day default** — no `--storage.tsdb.retention` flag is set.
   Confirm before relying on a rollup window wider than that.
4. **Whether `default-host` and `fallback` logs are worth ingesting at all.** They are scanner
   noise and TLS junk. Probably a separate, cheaper table — decide with the data in hand.

---

## 15. The AI platform's own database — and reversing §4/§11

**The owner's decision, 2026-09-14: the warehouse holds all of `ai_platform`, prompts and
responses included, not the trimmed columns §4 originally took.** This section is the
reversal §4 and §11 now point to, plus the build that followed from it (workstream
WS-41.E1).

**Why the reversal, stated plainly rather than left implicit.** §4's original call was made
when this repo was, first and foremost, a portfolio piece — §1 says so outright, and a
portfolio piece has an audience the owner's own conversations should never reach. That
reasoning held up until camera event summaries (§7: "describes people at the house"),
reverse-proxy visitor traffic and the full LLM spend ledger (§8, `metadata`, `messages`,
`response` already selected in `litellm_spend.py` since the ledger's own `messages`/`response`
columns are landed *whole*, empty only because LiteLLM's payload logging happened to be off)
were all already sitting in the same private warehouse, behind the same Keycloak-gated MCP
layer, read by nobody but the owner and whatever they ask the platform's own agents to do
with it. Excluding one more source's content at that point was not protecting anyone — the
lake was never published, §5's line about what leaves the repo was never about what enters the
warehouse — it was a rule that had outlived the reason it was written. The owner's Datalake
Analyst and the platform's own agents can now be asked "what did I actually ask Claude last
Tuesday" and get an answer, which was the entire point of the MCP layer in §10 existing.

**What did not reverse.** A prompt is content; a credential is not content, and the
2026-09-14 decision was never about credentials. `ai_platform_registry.py` excludes, by name,
every column that is a working secret rather than a fact about a conversation: `api_keys`'
hash and ciphertext, `providers`' encrypted credential, `push_subscriptions`' endpoint and
keys, `platform_settings.ntfy_topic`, `user_tool_configs.values`, and `mcp_servers.command`/
`.metadata` (a locally-spawned MCP server's launch command routinely carries a token as an
argument). `mcp_servers.url` is kept, with its query string stripped, the same boundary this
repo already applies to NPM request paths. None of this is discretionary the way the earlier
"leave the payload" call was — it is enforced twice: the registry never selects these columns,
and the read-only Postgres role's grant is generated *from* the registry, column-level, so a
bug in the extraction code could not read one even by mistake. See
`infra/ai-platform-datalake-role.sql` and the module docstring in `ai_platform_registry.py`
for the full list and the reasoning behind each entry.

**The scale this actually is.** Measured 2026-09-14: 52 tables (of 54 in the schema — the
Prisma migration ledger and a one-off backup table are skipped, not silently, by name, in
`SKIPPED_TABLES`). `usage_events` alone: [count withheld] gateway calls, [volume withheld], most of that [count withheld]
camera frames base64-encoded into a JSON column rather than landed as text — those are decoded
to individual JPEG objects in the lake at ingestion and referenced from the row by
`frame_keys`, not carried as a giant string. `run_steps`: [count withheld] tool calls in the trailing week
alone, none of which ever touch the LiteLLM ledger, because a tool call is not a gateway call.

**The design, briefly — full detail lives in the code, which is where it has to stay correct.**
One registry (`ai_platform_registry.py`) lists all 52 tables once: a primary key, a change
column or `MODE_SNAPSHOT`, the exact `SELECT` list, and any binary/redaction handling. Twenty-
eight config/entity tables (`agents`, `applications`, `schedules`, and so on) are re-read whole
every day, the same choice this repo already made for `jellyfin_titles` and `camera_zones`;
twenty-four event tables (`usage_events`, `runs`, `messages`, `run_steps`, and so on) are
windowed on a change column with a seven-day trailing re-extraction, so a row that changes
shortly after landing — a run completing, an approval being decided — is corrected on its next
re-run rather than frozen at its first-seen values. `message_attachments.data` and
`generated_files.data` are read, written to the lake as one object per row, and replaced in
the landed row by `object_key`; the same shape as the camera frames, generalised. dbt lands
six marts — `fct_ai_gateway_call`, `fct_agent_run`, `fct_agent_tool_call`,
`fct_conversation_message`, `fct_email_send`, `fct_automation_run` — over thirteen staging
models, and the MCP semantic layer exposes six matching datasets, so a model reading them is
told, in the dataset description itself, that a streamed call can report zero tokens and that
a camera frame is a file referenced by a key rather than a value in the row.

`fct_ai_gateway_call` and its staging model are deliberately absent from the lake's
curated/marts Parquet republish (`layout.CURATED_DATASETS` / `MART_DATASETS`): they carry the
same unbounded, ever-growing prompt/response text as `usage_events` itself, and that
republish rewrites its *entire* history to Parquet every night. Both are still built by dbt
and served through the warehouse and the semantic layer regardless — what they don't get is a
second copy dumped to the lake on a schedule that never stops adding to it. `Dataset.lake_only`
(added to `layout.py` on `platform_usage_events` alongside this work, on the same axis but one
step earlier in the pipeline — whether a raw table is rebuilt into the nightly DuckDB
warehouse at all) is the flag for the raw side of the same problem; no enforcement for it
ships in this change, so `platform_usage_events` is still loaded in full today, same as before
the flag existed. It is set as the decision for whoever wires the enforcement up.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
