# homelab-datalake

A lakehouse over a self-hosted homelab. Dagster lands daily partitions of reverse-proxy
logs into Parquet on MinIO; dbt models them in DuckDB; tests fail the build rather than
log a warning.

**Four sources live** — reverse-proxy logs, the LiteLLM spend ledger, the media library
and camera events — **and an MCP server on top**, reachable both directly and as a
registered upstream on the homelab's LiteLLM MCP gateway, so a model can ask the warehouse a
question in words and get back a figure or a formatted Excel workbook.

This repo publishes the code, a synthetic seed and the reasoning. It does not publish the
data — see [What is and isn't here](#what-is-and-isnt-here).

**[docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md)** is the short version: the lake layout, the
assets, the SQL console and what to do when something fails, in Simplified Technical
English. Read that first.

```
NPM access logs            ┐
LiteLLM spend ledger       ├→ redact at the boundary → Parquet on MinIO
Jellyfin library           │
Camera events (HTTPS+OIDC) ┘
                            → DuckDB → dbt staging / intermediate / marts
                            → tests that fail the build
                            → serving copy (marts only, read-only)
                            → MCP: typed tools, Keycloak token, .xlsx out
```

## Run it in two minutes, with no homelab

Everything below works on a fresh clone. CI runs exactly these steps, so a green badge
means the pipeline works, not that the interesting parts were skipped.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" "dbt-core>=1.9,<2" "dbt-duckdb>=1.9,<2"

export CLIENT_IP_HASH_SALT=$(openssl rand -hex 32)
export DATALAKE_LOCAL_ROOT=.lake DAGSTER_HOME=$PWD/.dagster_home
export NPM_LOG_DIR_PRIMARY=seed/logs/primary NPM_LOG_DIR_SECONDARY=seed/logs/secondary
export DUCKDB_PATH=$PWD/warehouse/datalake.duckdb DBT_PROFILES_DIR=$PWD/dbt
mkdir -p "$DAGSTER_HOME"

python scripts/generate_seed.py --days 3
# The manifest dagster-dbt reads to build one asset per model. Before any dagster
# command: without it the code location cannot load.
(cd dbt && dbt deps && dbt parse)

dagster asset materialize --select npm_access_raw --partition "2026-08-27|primary" \
  -m homelab_datalake.definitions

# Warehouse, dbt models, tests and the serving copy -- the job that runs nightly.
dagster job execute -j analytics_daily -m homelab_datalake.definitions
```

Then ask it something, over stdio, with no Keycloak and no server:

```bash
export DUCKDB_SERVING_PATH=$PWD/warehouse/serving.duckdb
python -c "
import asyncio, json
from homelab_datalake.mcp.server import server
r = asyncio.run(server.call_tool('web_traffic', {
    'start': '2026-08-25', 'end': '2026-08-27', 'grain': 'day',
    'measures': ['requests', 'visitors', 'server_error_pct']}))
payload = r.structured_content or json.loads(r.content[0].text)
print(json.dumps(payload['rows'], indent=2))
"
```

```json
[
  {"period": "2026-08-25", "requests": 816, "visitors": 8, "server_error_pct": 9.314},
  {"period": "2026-08-26", "requests": 816, "visitors": 8, "server_error_pct": 13.603},
  {"period": "2026-08-27", "requests": 816, "visitors": 8, "server_error_pct": 10.294}
]
```

For the asset graph, per-partition status and the backfill dialog:

```bash
dagster dev -m homelab_datalake.definitions
```

> **macOS note.** `pip install dbt-duckdb` can fail with `CERTIFICATE_VERIFY_FAILED` while
> fetching a build dependency. It is the system trust store, not the package:
> `export SSL_CERT_FILE=$(python -c "import certifi; print(certifi.where())")` fixes it.
> The Docker image pins Python 3.12 and avoids the problem entirely.

## The decisions, and why

Long form with numbers in [IMPLEMENTATION_PLAN.md §3](IMPLEMENTATION_PLAN.md). The short
version, and the measurements each rests on:

| | |
|---|---|
| All sources | **~[volume withheld]/day ≈ [volume withheld]/year** raw, ~1–[volume withheld]/year as Parquet |
| The server | 8 cores, [volume withheld] RAM (16 free), [volume withheld] disk |

**Five years of this warehouse fits in the free RAM of the machine that produces it.**
Most of what follows is a consequence of that.

**Parquet on object storage, not HDFS and Spark.** Spark buys parallelism across machines
and charges coordination for it. At [volume withheld] a day, JVM start and executor allocation cost
more than the work. HDFS's three jobs — files bigger than one disk, replication for
durability, compute locality — are all moot on one host with one disk. Parquet is the
portable half of that stack and costs nothing: if volume grows a hundredfold, point Spark
at the same bucket. **The format is the expensive decision to reverse, and it is already
made in Spark's favour.**

**DuckDB, not Postgres.** The workload is scan-a-date-range-and-aggregate. Columnar means
a two-column aggregate over a twenty-column table reads two columns; dictionary encoding
collapses the repetitive `host` and `user_agent`; Parquet row-group statistics prune
before any bytes are read. Postgres is row-oriented, so it reads all twenty — and it is
the shared database behind Keycloak and the AI platform, so a bad analytical query there
has a blast radius that includes the front door. DuckDB is single-writer and no kind of
serving layer; both are fine for a nightly batch and would be disqualifying under
streaming ingest.

**Daily batch, not streaming.** Every question here is day-grain: traffic per service last
month, spend in August, titles added this quarter. The one genuine real-time path —
motion detected, notification pushed — already exists inside the camera app, where the
context is. A lake that also alerts is a slow alerting system and an unreliable
warehouse. Batch also makes the properties this repo is meant to demonstrate nearly free:
idempotent re-runs and backfill are one line each in batch, and are watermarks, state
stores and log replay in streaming.

Not purely batch, and the plan says so: the camera sensor re-materialises the current day
when new events land, which is micro-batch at a fifteen-minute cadence. Freshness is set
per source by what its questions need.

**Dagster, not Airflow or cron.** The concrete reason: **NPM's log files do not map to
days.** Rotation is weekly, one file spans several dates, and a given day's lines can be
in the live log or any of four gzipped siblings. So nothing on the filesystem answers "is
14 August done?" — partition state has to live somewhere. Dagster tracks `(asset,
partition)` as a first-class fact with a backfill UI over it; with Airflow that
bookkeeping is yours to write and keep correct. Airflow remains the right call for a team
already running it.

## The MCP layer: typed tools, not text-to-SQL

A model can ask this warehouse a question and get back a number or a formatted
workbook. The interesting decision is what the tools *are*.

**Not `run_sql(query: str)`.** That is the version you can build in a weekend, and it is
the wrong one:

- A string parameter is an unbounded interface. Anything the model can write, the
  database will try — and the failure is silent, because a wrong join returns a number
  rather than an error.
- The schema becomes the API. Rename a column and you break callers you cannot see.
- "Which of these?" is a task models are reliable at. "Write correct SQL against a
  schema you have not seen" is one they are *plausible* at, which is worse.

So a question is a typed structure — dataset, date range, grain, dimensions, measures,
filters — and every name is looked up in
[a registry](src/homelab_datalake/mcp/semantic.py) while every value is a bound
parameter. **The model never supplies a fragment of SQL.** Injection is not mitigated,
it is unrepresentable: there is no path from model output to query text.

The cost is real and worth saying out loud: a question nobody anticipated needs a change
to the registry rather than a cleverer prompt. That is the trade — a narrower surface
that is right, over a wider one that is usually right.

```
llm_spend        spend, tokens, latency, failures — by model, provider, workload
web_traffic      requests, visitors, error rates, geography — by host, status, client
camera_activity  events, confirmations, confidence — by camera, zone, hour of day
media_library    titles, runtime, resolution, ratings — one snapshot, never summed
export_workbook  any of the above, as .xlsx with a provenance sheet
list_datasets / describe_dataset   what exists, and what values each dimension takes
```

**What stops a model running a cartesian join at 3am.** There are no joins to ask for —
a question addresses exactly one mart. Beyond that: a row cap applied as `LIMIT` so rows
are never materialised and discarded; a date-range cap; and a wall-clock deadline
enforced by a watchdog that calls `interrupt()`, because DuckDB has no statement timeout
and a deadline nothing enforces is a comment. Measured: a runaway scan is cancelled in
1.5s and the connection survives it.

**The empty answer gets the most care**, because it is the failure mode that does not
look like one. A model narrating a confident summary over zero rows is worse than an
error. So an empty result says it is empty, says which range *does* hold data, says
whether the requested window missed it entirely, and says not to estimate:

```
No rows matched. This is an empty result, not a zero.
camera_activity holds data from 2026-08-07 to 2026-08-27.
The requested range lies entirely outside that, so the window is the likely mistake.
Do not summarise or estimate figures for this period.
```

Exports refuse outright rather than produce a sheet of headers with no rows — to a
person that reads as "the answer is zero", which is a different claim.

**A snapshot is not a stream of events.** `media_library` describes a moment, so a date
range selects *which* snapshot to report rather than a span to aggregate. Summing titles
across days would count each film once per day it sat on the disk — a number that looks
like growth and is arithmetic.

**Deliberately not exposed:** `client_hash` identifies one visitor across requests. It is
in the fact table because visitor *counts* need it, but it is not a dimension — grouping
by it would turn an aggregate tool into a per-person report, which is what the ingestion
boundary exists to prevent. [A test](tests/test_mcp_query.py) fails if anyone adds it
back for convenience.

### The serving copy, and a stale-read that nothing would have reported

DuckDB allows one writing process. Measured, not assumed: a **read-only** open also
fails under that lock —

```
IO Error: Could not set lock on file ".../datalake.duckdb"
```

— so a reader pointed at the warehouse would not merely see stale data during the
nightly build, it would raise for the length of it. Instead the build writes
`datalake.duckdb`, and on success the marts are copied into `serving.duckdb` and swapped
in atomically. Readers never contend with the build, and a failed build never reaches
them.

Two properties fall out, and both were the point:

- **Only the marts are copied.** Not raw, not staging. "The serving layer cannot see raw
  data" is a fact about the file rather than a promise about the code.
- **The semantic layer is bound against the staged copy before the swap.** A dbt column
  rename fails the deploy, with the previous copy still serving — instead of failing the
  first question that happens to touch that column, weeks later.

The subtle part: the serving copy is opened with `ATTACH`, not `duckdb.connect(path)`.
DuckDB caches database instances **by path within a process**, so once one handle is
open, a later `connect()` to the same path joins the cached instance rather than reading
the file. After the nightly swap that means a "fresh" connection keeps returning the
previous build's numbers for as long as any other handle lives — and **nothing raises**.
Measured at 200 stale rows against 89 correct ones, and
[pinned by a test](tests/test_serving.py) so it cannot be simplified away.

### Authentication

Bearer tokens against the same Keycloak realm as everything else, so revoking the
pipeline's access is a toggle beside every other account rather than an edit to a file on
a server. The token arrives from the caller, so its signature is the only thing
separating a real client from someone who wrote their own JSON — it is verified against
the realm's published keys on every request, never decoded and trusted. Authorisation is
the `datalake:query` **client** role, which a realm role of the same name cannot satisfy.

`/mcp` bypasses oauth2-proxy deliberately: its callers are programs, and a browser login
flow is not something they can complete. It is authenticated differently, not less. The
server refuses to start over HTTP with no issuer configured — an unauthenticated
analytics endpoint is a worse outcome than one that is down, because nothing about it
looks wrong.

Verified from the public internet: no token → **401**; a garbage token → **401**; a
forged token carrying perfect claims but signed with a key the realm never published →
**401**.

## The parts worth reading

**Three log formats, dispatched by shape.**
[`parsing/npm_log.py`](src/homelab_datalake/parsing/npm_log.py) handles NPM's `proxy` and
`standard` formats plus stock nginx `combined`. Validated against real logs from both
hosts: **[count withheld] lines parsed in 0.55 s, zero rejects** — [volume withheld] across 12 files on one core.
That is the whole daily job, and it is the number behind "Spark's coordination would cost
more than the work". A line nothing matches is kept with its text, and
[a dbt test](dbt/tests/assert_reject_rate_within_threshold.sql) fails the build above [rate withheld].
Silently dropping what you stop understanding is how a pipeline lies while the dashboard
stays green.

**The partition is the timestamp inside the line, never the filename.**
[`parsing/extract.py`](src/homelab_datalake/parsing/extract.py) reads the live log plus
every rotated sibling and filters by parsed timestamp. Slower than trusting a filename,
and the only version that survives a rotation landing mid-run. Files last modified before
the partition day are pruned — on the camera host that is the difference between reading
[volume withheld] and reading the ~[volume withheld] that could possibly contain the day.

**Enrich, then discard.** [`privacy.py`](src/homelab_datalake/privacy.py) is the only
place a raw client address exists, and only in memory. Country, city and ASN are derived
while the address is still in hand; then it becomes a salted HMAC and a /24 or /48
network, and the original is dropped. Every chart survives; no identifier does. There is
no raw landing zone, because that zone is the leak. A
[dbt test](dbt/tests/assert_no_raw_ip_addresses.sql) re-checks the fact table, so a future
column cannot reintroduce one.

Verified against real traffic, not only fixtures: a day of production logs containing 57
distinct client addresses produced [count withheld] rows, **none of which contained any of them**.

**The same rule, turned on the repository.** The pipeline was careful and the repo was not:
three real addresses reached git — two fixtures lifted from production logs while writing
the very suite that asserts addresses never leak, and a scanner's address pasted into a
document as an example log line. Two were replaced a few commits later, which felt like a
fix and was not one. A deleted line is still in history, and history is what gets cloned.

So history was rewritten to remove all three, and
[the guard](tests/test_no_real_addresses_in_fixtures.py) now scans **every blob in every
commit**, not just the working tree — the check that would have caught them. Anything
outside RFC 5737, RFC 3849, private space and Cloudflare's published ranges fails the
build. CI checks out at full depth for it, because a shallow clone would make that test
skip silently and report green.

**The LLM ledger: select the columns, leave the payload.**
[`parsing/litellm_spend.py`](src/homelab_datalake/parsing/litellm_spend.py) reads [count withheld]
calls ([spend withheld] since 16 June) with model, tokens, latency and cost. `messages`,
`response` and `proxy_server_request` hold whole conversations and are excluded **by
name**, not because they happen to be empty today — switching payload logging on must
not silently start ingesting people's chats, and
[a dbt test](dbt/tests/assert_no_prompt_payloads_in_warehouse.sql) fails if any of those
names ever appear on the fact table. The API key is hashed too: the column holds both
64-character hashes and 24-character literals, and nothing in it says which is which, so
it is treated as a credential. The Postgres role names its one table rather than being
granted the schema.

**The library: a star schema where one is actually the right shape.**
[`parsing/jellyfin.py`](src/homelab_datalake/parsing/jellyfin.py) snapshots [count withheld] movies
daily. Read through the API rather than the CSV export because `ProviderIds.Tmdb` comes
back on **[rate withheld]** of titles, making enrichment a key lookup instead of fuzzy title
matching — which is what gets remakes and translated titles wrong. Genres are a
[bridge table](dbt/models/marts/bridge_title_genre.sql), not `genre_1/genre_2/genre_3`:
a title averages three and some carry six. Resolution is bucketed by *range*, because
real heights here are 1080, 1040, 1036, 816, 808, 804 — cropped and letterboxed encodes
that equality bucketing would scatter across a dozen classes.
[SCD2](dbt/models/marts/dim_title_scd2.sql) is a pure function of the Parquet
partitions rather than dbt's stateful snapshot materialisation, so dropping the
warehouse and rebuilding gives identical history. Two tests hold it: versions may not
overlap, and exactly one may be current per title.

Never stored: file paths (in this library they carry the name of the site each file came
from), and playback history.

**Bad source data is flagged, never corrected.** Two `mpeg2video` DVD rips report [count withheld]
and [count withheld] minutes for films of about 110 and 87 — wrong container metadata. The value is
kept as reported and marked `is_runtime_plausible`; inventing a believable runtime would
put a fabricated number in the warehouse, and widening the test would hide it. A separate
test fails only if the share grows past [rate withheld].

**Camera events: another site, reached over its URL with a token.** The camera platform
runs elsewhere and is read the way anything else would be — its public URL with a
Keycloak bearer token — not by opening a tunnel between the two sites. The token is a
`client_credentials` grant carrying an `export:read` client role, so access is revoked
in Keycloak beside every other account.

The export endpoints
([`export-auth.js`](https://github.com/Franciskid/homelab-camera), `export-routes.js` in
that repo) verify the token's **signature** on every request. The camera app's existing
auth reads JWT claims without verifying them, which is correct where it sits — that
token came straight from Keycloak over TLS during a login redirect. Here the token
arrives from whoever made the request, so the signature is the only thing between a real
caller and someone who wrote their own JSON. Verified against all three cases: no token
→ 401, forged signature → 401, real token → 200.

Exports hand over analytics, not the house: no zone geometry (those polygons are a map
of the property), no media, and `createdBy` becomes a salted hash.

**Late-arriving analysis, described accurately.** Vision analysis lands hours after the
event, so an hourly schedule re-reads the last two days. Re-materialising *replaces* a
partition, which is what keeps the marts current — the dedup in
[`int_camera_events`](dbt/models/intermediate/int_camera_events.sql) is a guard against a
partition landing as several objects, not the mechanism, and measurement says so: three
re-reads collapsed zero rows.

**Partitioned by date *and* host.** The two proxies are on different machines and fail
independently. `(2026-08-27, secondary)` breaking must not mark `(2026-08-27, primary)` broken,
and re-running one must not touch the other.

**Re-running a partition replaces it exactly.** One object per partition, one PUT, so the
write is atomic without rename semantics S3 does not have. CI asserts it by hashing the
Parquet before and after a re-run.

## Real client addresses: [rate withheld] of the column was a CDN

Before 27 August 2026, **84–[rate withheld] of the logged client address was a Cloudflare edge rather
than a visitor** ([count withheld] of [count withheld] lines sampled). Not because the ranges were untrusted —
NPM already trusted all 21 Cloudflare publishes — but because it read `X-Real-IP` and
Cloudflare sends `CF-Connecting-IP`. One header name.

The fix is [one directive](infra/npm/server_proxy.conf) in the server context, which NPM
includes in every proxy-host block. Measured across all 44 hosts:

| | Requests | Cloudflare edges |
|---|---|---|
| Before 21:27 UTC | [withheld] | [count withheld] (**[rate withheld]**) |
| After | [withheld] | 0 (**[rate withheld]**) |

The five weeks already on disk stay wrong forever, so every row carries `client_trust` —
`resolved` or `cdn_edge` — and `visitors` counts only `resolved` rows. Two values, not
three: after the cutover a restored address and an always-direct one are byte-identical,
and claiming to tell them apart would assert something the data does not contain.
`scripts/verify-real-ip.sh` reprints the share by hour.

## A bug the tests caught

DuckDB casts a `TIMESTAMP WITH TIME ZONE` to a date **in the session timezone**. On a
Europe/Paris machine a request logged at 23:30 UTC landed on the next day, so the same
backfill would have produced different partitions on a laptop and on the server — breaking
idempotency outright, silently. UTC is now pinned in both `dbt/profiles.yml` and the
Dagster connection, and
[the test that caught it](dbt/tests/assert_partition_date_matches_timestamp.sql) now guards
the pin.

## Operating it

Runs on the server, next to the sources — the UI is at **https://data.example.test**. See
[IMPLEMENTATION_PLAN.md §5](IMPLEMENTATION_PLAN.md) for per-service access.

```bash
cp .env.example .env      # set CLIENT_IP_HASH_SALT and the MinIO key
docker compose up -d --build
```

**The UI is behind Keycloak, and that is load-bearing.** Dagster's webserver has no
authentication of its own: anyone reaching it can materialise assets, launch backfills
and read run logs. So it is published on no host port at all, and the only route in is
nginx → oauth2-proxy → Dagster. oauth2-proxy completes an OIDC login against the homelab
realm with PKCE and forwards only sessions belonging to a user holding the
`datalake-user` role.

Identity lives in one place rather than in an htpasswd file nobody remembers: access is
granted and revoked in Keycloak, next to every other account. Grant someone access by
assigning them `datalake-user`; revoke by removing it.

**`/mcp` is the one path that does not go through oauth2-proxy**, because its callers are
programs and a browser login is not something they can complete. It verifies a
Keycloak-signed bearer token carrying the `datalake:query` client role on every request,
and refuses to start over HTTP if no issuer is configured. Setting it up needs one
confidential client, `datalake-mcp`, with service accounts enabled and a client role
`datalake:query` assigned to its own service account — the same shape as
`datalake-exporter`, which the camera export uses.
[`scripts/setup-keycloak-mcp.sh`](scripts/setup-keycloak-mcp.sh) creates it; it prompts
for realm-admin credentials rather than reading them from anywhere.

The vhost is [`infra/npm/http.conf`](infra/npm/http.conf), installed to NPM's custom
include — a path NPM never rewrites, unlike the `proxy_host/<id>.conf` files it
regenerates from its own database. That keeps the vhost in version control instead of
only in NPM's SQLite.

- NPM's log volume is mounted **read-only**. A pipeline that can corrupt the reverse
  proxy's logs is a pipeline that can take every service on the box offline.
- **Only the instances actually mounted are ingested.** `NPM_HOSTS` decides, and it
  defaults to `primary` alone. The two sites are deliberately not joined at the host
  level — no cross-site SSH, no shared filesystem. Anything read across them goes over
  its public URL with a Keycloak token, the way the camera source does.
- **Backfill reaches about five weeks and no further.** NPM rotates weekly and keeps four
  generations. Once this runs, the lake is the only long-term history — which is a real
  reason for it to exist.
- Rotating `CLIENT_IP_HASH_SALT` breaks continuity in visitor counts. That is correct
  behaviour, so make it deliberate and dated.
- **Enrichment is optional, and deliberately small.** Without a MaxMind database the geo
  columns are null. Without a `TMDB_API_KEY`, five columns are null — budget, revenue,
  popularity, vote count and original language — and *nothing else*, because Jellyfin
  already queried TMDB when it matched each file. Measured across [count withheld] titles: the
  community rating (which **is** TMDB's vote average) [rate withheld], original title [rate withheld],
  production country [rate withheld], release date [rate withheld]. An earlier version of this fetched all
  of those again and left them null pending an API key, which was simply wrong: it put
  a credential between the warehouse and data already on disk. What remains is a *key*
  lookup on the id Jellyfin stored, never a title search, so a film cannot pick up
  another film's box office.
- **An empty log directory fails the run.** It means a broken mount or a stalled sync,
  and a partition that is green and empty is indistinguishable from a quiet day. A run
  that is green and wrong is worse than one that fails.

**The nightly chain.** Sources land between 03:00 and 03:45; `analytics_daily` at 04:15
rebuilds the warehouse, builds and tests every dbt model, and publishes the serving copy.
Half an hour of slack rather than the couple of minutes the runs take, so a retry on a
slow source still finishes before the marts are built — a mart built from a partition
that had not landed yet is silently short by a day.

| | |
|---|---|
| `npm_daily` | 03:00 — yesterday's logs, one run per host |
| `litellm_daily` | 03:15 — yesterday's LLM spend |
| `jellyfin_daily` | 03:30 — today's library snapshot |
| `camera_daily` | 03:45 — yesterday's camera events |
| `camera_late_arrival` | hourly — re-reads the last two days, because vision analysis lands late |
| `analytics_daily` | 04:15 — warehouse, dbt, serving copy |

Before this existed the sources refreshed nightly and the marts refreshed whenever
somebody remembered to run `dbt build`. That is a way of being wrong that never announces
itself, and a serving layer over stale marts is worse than no serving layer.

## What is and isn't here

| | Published | Private |
|---|---|---|
| Pipeline code, dbt models, tests, CI | ✅ | |
| The MCP server and its semantic layer | ✅ | |
| Synthetic seed generator | ✅ | |
| The reasoning, with its numbers | ✅ | |
| Real logs, camera events, library data | | ✅ |
| Generated workbooks | | ✅ |
| Credentials, endpoints, network topology | | ✅ |

Every address in this repository is from a documentation range (RFC 5737, RFC 3849), is
private or reserved space, or is a published Cloudflare prefix. The seed is generated, not
sampled. That rule is [enforced by a
test](tests/test_no_real_addresses_in_fixtures.py) over every tracked file **and every
commit**, because reviewing by eye did not work: two fixtures were copied straight out of
the production logs while writing the suite that asserts addresses never leak, and a
third — a scanner's address pasted into a document as an example log line — sat in
history unnoticed until the check was widened to scan it.

## Layout

```
src/homelab_datalake/
  parsing/npm_log.py    three log formats, rejects kept not dropped
  parsing/extract.py    partition by timestamp, rotation-safe
  privacy.py            the PII boundary: enrich, hash, discard
  geo.py                MaxMind lookups, optional by design
  layout.py             every path in the lake, declared once
  migrate_layout.py     copy, verify, delete -- moving the bucket safely
  resources.py          MinIO or local disk behind one interface
  serving.py            the read-only marts copy, and why it exists
  assets/               partitioned assets, dbt models, the serving publish
  sql/                  the SQL console: engine, guards, one page
  mcp/semantic.py       what may be asked, in business vocabulary
  mcp/query.py          validation, SQL construction, the limits
  mcp/deliverables.py   .xlsx with a provenance sheet
  mcp/auth.py           Keycloak bearer tokens, verified per request
  mcp/server.py         the tool surface
dbt/models/             staging → intermediate → marts
dbt/tests/              the guarantees, as SQL that fails the build
scripts/                seed generator, real-IP verifiers, Keycloak setup
docs/HOW_IT_WORKS.md    the short version, in Simplified Technical English
infra/npm/              the deployed reverse-proxy config
```

The bucket has three layers, and the layer is the first path segment:

```
raw/       as landed          raw/infrastructure/npm/access/host=primary/dt=2026-08-27/
curated/   cleaned, enriched  curated/infrastructure/npm/requests/partition_date=2026-08-27/
marts/     facts and totals   marts/ai/agg_llm_spend_daily/partition_date=2026-08-27/
```

Under the layer the path is `<domain>/<source>/<dataset>`, with four domains —
`infrastructure`, `media`, `monitoring`, `ai`. A mart has no source segment, because it
joins more than one. Every path is built from `layout.py`; nothing else spells one out.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
