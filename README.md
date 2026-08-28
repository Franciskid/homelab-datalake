# homelab-datalake

A lakehouse over a self-hosted homelab. Dagster lands daily partitions of reverse-proxy
logs into Parquet on MinIO; dbt models them in DuckDB; tests fail the build rather than
log a warning.

**Stage 1 ships the reverse-proxy logs, end to end.** Camera events, the media library
and an LLM spend ledger come next — see [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

This repo publishes the code, a synthetic seed and the reasoning. It does not publish the
data — see [What is and isn't here](#what-is-and-isnt-here).

```
NPM access logs (2 hosts)
  → parse 3 formats → enrich geo/ASN → hash the address → Parquet on MinIO
  → DuckDB → dbt staging / intermediate / marts → tests that fail the build
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
dagster asset materialize --select npm_access_raw --partition "2026-08-27|primary" \
  -m homelab_datalake.definitions
dagster asset materialize --select npm_access_warehouse -m homelab_datalake.definitions
cd dbt && dbt deps && dbt build
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

Runs on the server, next to the sources. See
[IMPLEMENTATION_PLAN.md §5](IMPLEMENTATION_PLAN.md) for per-service access.

```bash
cp .env.example .env      # set CLIENT_IP_HASH_SALT and the MinIO key
docker compose up -d --build
```

- NPM's log volume is mounted **read-only**. A pipeline that can corrupt the reverse
  proxy's logs is a pipeline that can take every service on the box offline.
- The other host's logs arrive by `scripts/sync-secondary-logs.sh`, nightly, before the 03:00
  schedule.
- **Backfill reaches about five weeks and no further.** NPM rotates weekly and keeps four
  generations. Once this runs, the lake is the only long-term history — which is a real
  reason for it to exist.
- Rotating `CLIENT_IP_HASH_SALT` breaks continuity in visitor counts. That is correct
  behaviour, so make it deliberate and dated.
- Geo enrichment is optional. Without a MaxMind database the geo columns are null and
  everything else works — which is what keeps CI free of a licence key.

## What is and isn't here

| | Published | Private |
|---|---|---|
| Pipeline code, dbt models, tests, CI | ✅ | |
| Synthetic seed generator | ✅ | |
| The reasoning, with its numbers | ✅ | |
| Real logs, camera events, library data | | ✅ |
| Credentials, endpoints, network topology | | ✅ |

Every address in this repository is from a documentation range (RFC 5737, RFC 3849), is
private or reserved space, or is a published Cloudflare prefix. The seed is generated, not
sampled. That rule is [enforced by a
test](tests/test_no_real_addresses_in_fixtures.py) over every tracked file, because
reviewing by eye did not work: two fixtures were copied straight out of the production
logs while writing the suite that asserts addresses never leak.

## Layout

```
src/homelab_datalake/
  parsing/npm_log.py    three log formats, rejects kept not dropped
  parsing/extract.py    partition by timestamp, rotation-safe
  privacy.py            the PII boundary: enrich, hash, discard
  geo.py                MaxMind lookups, optional by design
  resources.py          MinIO or local disk behind one interface
  assets/               multi-partitioned assets and their checks
dbt/models/             staging → intermediate → marts
dbt/tests/              the guarantees, as SQL that fails the build
scripts/                seed generator, log sync, real-IP verifiers
infra/npm/              the deployed reverse-proxy config
```

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
