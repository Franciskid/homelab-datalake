# Metrics into the lake, streaming: audit and plan

Written 2026-09-15. Phases 0 to 2 and four of the section 6 sources shipped the same day;
see "Status" at the end.

The brief: Grafana's data should also be saved to the datalake. Ideally it streams through
Dagster so the newest samples can be queried live. Separately, list what else on primary
is not in the lake yet.

Every number below was measured against production on the night of 14–15 September 2026
(server time 22:00–22:30 UTC), not inferred.

---

## 0. Blocker: Prometheus forgets a day every day

Prometheus keeps **15 days** (the default, `storageRetention: 15d`). The oldest sample it
holds today is from 30 August, 13:00 UTC. Each day before a backfill runs, one more day of host,
container, GPU and LiteLLM history is gone for good.

It is also more fragile than 15 days suggests. The TSDB ([volume withheld]) lives in an **anonymous**
Docker volume (`b7f81628…`), because the compose file mounts only `/etc/prometheus`. A
`docker compose down` followed by `up` starts a new empty volume, and all 15 days are lost
at once.

So the backfill in phase 1 goes first. Optionally, while this plan is reviewed, the
completed TSDB blocks can be copied aside (read-only, [volume withheld]). They could then be replayed
later.

---

## 1. What Grafana actually shows

**Yes, everything in Grafana comes from Prometheus.** Grafana has exactly one data source,
`Prometheus → http://198.51.100.26:9090`, and four dashboards:

| dashboard | panels with queries | reads |
|---|---|---|
| Node Exporter Full (1860) | 249 | `node_*` |
| Nvidia GPU Metrics (14574) | 29 | `nvidia_smi_*` |
| CPU (hand-made, 2023) | 7 | `node_*` |
| AI Platform / AI Platform Overview | 5 | `up`, `platform_*`, `node_filesystem_*`, `DCGM_FI_DEV_GPU_UTIL` |

### 1.1 Prometheus

- `prom/prometheus:latest`, which resolves to **v2.48.0 (Nov 2023)** on this host. The
  next `docker compose pull` would jump to 3.x, which has breaking changes.
- Global scrape interval 15 s. [count withheld] jobs, all `up`. No `remote_write`, no remote-write
  receiver, admin API off.
- **[count withheld] active series, [count withheld] metric names, [count withheld] samples/s ≈ [count withheld] samples/day.**
- Metadata: 371 gauges, 225 counters, 15 histograms, 14 summaries, 47 untyped.

| job | target | interval | series | share of samples |
|---|---|---|---|---|
| cadvisor | cadvisor:8080 | 30 s | [count withheld] | 40% |
| node-exporter | node-exporter:9100 | 15 s | [count withheld] | 26% |
| node | node-exporter:9100 | 30 s | [count withheld] | 13% |
| ai-litellm | litellm-metrics:9091 | 30 s | [count withheld] | 8% |
| prometheus | localhost:9090 | 15 s | 856 | 8% |
| nvidia_smi_exporter | 198.51.100.26:9836 | **5 s** | 122 | 3% |
| gpu | nvidia_smi_exporter:9835 | 30 s | 122 | 0.6% |
| ai-platform-api | platform-api-metrics:4001 | 30 s | 10 | <0.1% |

### 1.2 Problems found on the way

1. **Two jobs are duplicates.** `node` and `node-exporter` scrape the same exporter. So do
   `gpu` and `nvidia_smi_exporter` (one by container name, one by LAN IP at 5 s). That is
   ~30% of all samples. The Nvidia dashboard has no job filter, so it draws each line
   twice.
2. **One Grafana panel is empty for good.** "GPU utilization %" on AI Platform Overview
   queries `DCGM_FI_DEV_GPU_UTIL`, a DCGM exporter metric. This host runs the nvidia-smi
   exporter, so the panel should read `nvidia_smi_utilization_gpu_ratio`.
3. **Only primary is monitored.** Secondary (secondary.example.test, the camera host) and Tertiary are
   not scraped. Their CPU, RAM and disk history exists only in status-page's
   `metrics.json` (see §6).
4. **A second metrics pipeline duplicates node-exporter.** homelab-backend publishes
   CPU/RAM every 2 s to Kafka topic `app-metrics`, then writes each message to Postgres
   `primary_homelab."Log"`. [count withheld] of that table's [count withheld] rows ([volume withheld]) are these samples.
5. **LiteLLM's metric labels carry personal fields.** `client_ip` (2 real values today),
   `user_email`, `end_user`, `user` (all empty today) and `user_agent`. The lake's privacy
   boundary must apply to label values as well as columns.

### 1.3 Measured cost of taking it out

From inside `datalake-daemon`, which reaches Prometheus at `198.51.100.26:9090`:

- Every raw sample of the last 5 minutes, all [count withheld] jobs: **0.5 s**, [count withheld] series, [count withheld]
  samples. The largest single response is cadvisor, [volume withheld] of JSON per minute.
- As Parquet (zstd), in the long shape `ts, series_id, metric, job, value`:
  **1.95 bytes/sample → [volume withheld]/day → [volume withheld]/year** at full resolution.
- The same data with the label set repeated on every row: 9.4 bytes/sample, [volume withheld]/day.
  So labels go in a series table, stored once. All [count withheld] label sets together are [volume withheld].
- Without the two duplicate jobs and Prometheus's own metrics: **[volume withheld]/day, [volume withheld]/year**.
- The host has [volume withheld] free (72% used).

### 1.4 Dagster constraints that decide the design

- Dagster 1.13.20, `QueuedRunCoordinator` + `DefaultRunLauncher`, run monitoring **off**,
  no retention settings.
- A run takes **5–9 s just to start** (measured on the hourly `camera_daily` runs), then
  ~6 s to do its work. One run per minute would be [count withheld] runs a day, mostly start-up
  overhead, and each writes its event log to the shared Postgres.
- `SensorResult` in 1.13 accepts `cursor`, `run_requests` and `asset_events`
  (materializations and check evaluations). A sensor can do a small unit of work, record
  where it got to, and report it to the asset graph without launching a run. The default
  sensor evaluation timeout is 60 s (`DAGSTER_SENSOR_GRPC_TIMEOUT_SECONDS`).

---

## 2. Design principle: Prometheus is already the buffer

Streaming systems need a durable buffer so a consumer can stop and resume without losing
data. This host already has one: Prometheus's TSDB holds every raw sample for 15 days.

So every option below is built on the same rule. **Keep a cursor, read raw samples from
the cursor up to *now − 60 s*, write them, then move the cursor.** If Dagster is down for
an hour or a week, the next tick continues from the cursor. The same code backfills the 15
days that exist today.

**Raw samples, not `query_range`.** `query_range` returns values interpolated at a step,
which are not the scraped values. An instant query with a range selector,
`{job="cadvisor"}[60s]` evaluated at `time=end`, returns every scraped sample in the window
with its own timestamp. In 2.x the window is closed at both ends, and 3.x opens the left
end. Deduplicating on `(series_id, ts)` makes that difference irrelevant.

**Why the 60 s lag.** A sample is timestamped when its scrape starts and appended when the
scrape ends (up to the 10 s scrape timeout). Reading closer to *now* could advance the
cursor past a sample that has not landed yet.

---

## 3. The streaming part: three ways, one recommended

Dagster is an orchestrator, not a stream processor, and it has no streaming asset type.
Something has to hold the loop.

### Option A: a sensor that micro-batches (recommended)

A sensor `prometheus_stream`, `minimum_interval_seconds=60`. Each tick:

1. reads the cursor (or recovers it from the newest part file's `max(ts)` if it is missing:
   no second state store),
2. pulls `(cursor, now − 60 s]` per ingested job (≈27–[count withheld] samples, ~1–2 s),
3. applies the label privacy rules, computes `series_id`, splits the batch at hour
   boundaries, and writes one Parquet part per hour touched with a single PUT,
4. returns the new cursor, with a tick message such as
   `wrote [count withheld] samples through 22:14:00Z, lag 71 s`.

Guard rails:

- **A capped tick.** A tick never pulls more than 15 minutes of data, so it stays well
  under the 60 s timeout. After a longer outage the sensor returns `RunRequest`s for the
  missing whole hours to the backfill job (§4.3) and moves its cursor past them.
- **Failures reach a run.** Sensor exceptions do not trigger `notify_on_failure`. The
  hourly compaction run (§4.2) therefore *fails* when the cursor is more than 10 minutes
  behind. That failure is an ordinary run failure, so it reaches the AI platform incident
  channel that already exists.

Latency: a sample is queryable 1–2 minutes after it is scraped. No new container, no new
infrastructure, and nothing to keep alive.

### Option B: one long-running Dagster run

A job whose op loops for ever: pull every 15 s, flush every 60 s. A sensor restarts it
when it is not running, and a concurrency limit of 1 stops a second copy.

It gives live logs in the UI. However, a `DefaultRunLauncher` run dies whenever the daemon
container restarts, and with run monitoring off, the dead run stays `STARTED` and holds the
concurrency slot for ever. This option needs run monitoring on, a restart after every code
deploy, and a run in the UI that never ends. It is no faster than A for a Parquet lake,
because the flush interval sets the latency.

### Option C: push through Kafka (optional phase 4, the "real streaming" project)

Prometheus `remote_write` → a small relay → Kafka topic `metrics.prometheus` → a Dagster
consumer (option B's shape) → Parquet.

- Seconds into Kafka (remote write batches for up to 5 s). Kafka offers replay and more
  consumers, and the consumer could also land the `app-metrics` topic that already exists.
- **Volume, not measured yet.** One Kafka message per sample would be [volume withheld]/day, and
  Kafka's default 7-day retention would hold [volume withheld]. Forwarding each remote-write request
  as one message helps, but remote write 1.0 repeats every series' full label set in every
  request. So expect GBs per day even compressed. The topic needs a short retention
  (~24 h), or the relay re-encodes to `(series_id, ts, value)`. Measure before choosing.
- **Remote write does not backfill**, so options A and C both need the pull path, for the
  first 15 days and to repair gaps.
- **Kafka needs hardening first.** It has no authentication today.
- Lake latency drops from ~1–2 min to ~30–45 s. That is a real gain for a dashboard and
  a small one for SQL.

**Recommendation: build A.** The writer, compaction, checks and tables are the same in all
three options, so C can be added later without discarding anything.

---

## 4. Lake design

### 4.1 Datasets

Registered in [layout.py](../src/homelab_datalake/layout.py), domain `infrastructure`.
The domain comment widens from "the edge" to "the edge and the machines behind it".

| table | path | grain | notes |
|---|---|---|---|
| `raw.prometheus_samples` | `raw/infrastructure/prometheus/samples/dt=…/` | one scraped sample | `ts timestamp(ms, UTC)`, `series_id int64`, `metric`, `job`, `value double`. Sorted by `metric, series_id, ts` inside a file. |
| `raw.prometheus_series` | `raw/infrastructure/prometheus/series/dt=…/` | one label set per day seen | `series_id`, `metric`, `job`, `instance`, `labels MAP(VARCHAR, VARCHAR)`, `first_seen`, `last_seen` |
| `raw.prometheus_metadata` | `raw/infrastructure/prometheus/metadata/dt=…/` | one metric per day | `type` (counter/gauge/histogram/summary), `help`, `unit`. It is how a model knows what to `rate()`. |

`series_id` is a 64-bit hash of the sorted label set **after** redaction. So a hashed
`client_ip` never produces two series for one address.

**Label privacy, at ingest.** Through `privacy.py`, `client_ip` becomes the same keyed
pseudonym NPM and LiteLLM use, and `user_email`, `end_user` and `user` are hashed when not
empty. `instance` values that are IP addresses are rewritten to the job's target name, so
`assert_no_raw_ip_addresses` stays meaningful. Labels are never dropped silently: any
label that is rewritten is named in the series row.

**Job coverage, not a silent allowlist.** An `INGESTED_JOBS` / `SKIPPED_JOBS` pair, each
with a reason, like the semantic-layer coverage rule. A check fails when Prometheus
reports a job that is in neither list. Suggested default: ingest `node`, `gpu`,
`cadvisor`, `ai-litellm` and `ai-platform-api`. Skip `node-exporter` and
`nvidia_smi_exporter` (duplicates) and `prometheus` (self-metrics).

### 4.2 File lifecycle: parts → hour → day

One partition column (`dt`), three kinds of file. Mixing `dt=/hour=/` depths would break
DuckDB's hive partitioning when a view unions them.

| file | written by | lives until |
|---|---|---|
| `dt=D/part-<end_ms>.parquet` | sensor, every tick | its hour is compacted (≤ 60 per hour) |
| `dt=D/hour-HH.parquet` | `prometheus_samples_hourly`, at HH+1:07 | the day is compacted (≤ 24 per day) |
| `dt=D/data.parquet` | `prometheus_samples_daily`, at 00:20 | for good |

A query over today opens at most ~84 files. A query over a past day opens one.

**Compaction** reads the files, deduplicates on `(series_id, ts)`, writes the merged file
with a single PUT, and only then deletes the inputs. It refuses to compact an hour the
cursor has not passed.

Asset checks on the hourly asset:

- `hour_matches_prometheus`: the sample count equals Prometheus's `count_over_time` for
  that hour and those jobs, within 0.1%. This is completeness measured against the source,
  not against itself.
- `no_duplicate_samples`.
- `cursor_is_fresh`: fails the run when the cursor is more than 10 minutes behind.

**Bucket versioning.** The lake bucket keeps non-current versions for 30 days. Compaction
deletes about two days' worth of small files every day, which would add ~4–[volume withheld] of dead
versions. A lifecycle rule scoped to `raw/infrastructure/prometheus/`, with
`NoncurrentDays: 1`, stops that. It is added through
[enable-lake-versioning.sh](../scripts/enable-lake-versioning.sh), which already refuses
any rule that expires current versions.

### 4.3 Backfill and repair

`prometheus_samples_backfill`: an `HourlyPartitionsDefinition` job that writes
`hour-HH.parquet` directly. It pulls in 10-minute windows per job so no response exceeds
[volume withheld]. 15 days is ~11–[count withheld] requests, each under 0.5 s, so about 1–2 hours in total.

The same job is how the sensor fills a long gap, and how anyone repairs an hour that
`hour_matches_prometheus` flagged, while that hour is still inside Prometheus's retention.

### 4.4 Querying it live

Nothing new is needed. [api/query.py](../src/homelab_datalake/api/query.py) already creates
`raw.<table>` as `read_parquet('<prefix>/**/*.parquet')` for every registered dataset, so
each new part file is visible on the next statement from the Data workspace or Python.

```sql
-- CPU busy % per minute, last hour, straight off the stream
with cpu as (
  select s.ts, s.series_id, s.value, r.labels['mode'] as mode
  from raw.prometheus_samples s
  join raw.prometheus_series r using (series_id)
  where s.metric = 'node_cpu_seconds_total'
    and s.dt = current_date and s.ts > now() - interval 1 hour
)
select time_bucket(interval 1 minute, ts) as minute,
       1 - sum(inc) filter (where mode = 'idle') / sum(inc) as cpu_busy
from (select *, value - lag(value) over (partition by series_id order by ts) as inc from cpu)
where inc >= 0          -- a negative increase is a counter reset
group by 1 order by 1;
```

### 4.5 The warehouse must not load raw samples

Every raw dataset today is copied into the DuckDB warehouse by
[warehouse.py](../src/homelab_datalake/assets/warehouse.py). At [count withheld] rows/day that would
multiply the warehouse file's size within a week.

`Dataset` gains `lake_only: bool` with a stated reason. In the warehouse, a lake-only
dataset is a **view** over the Parquet glob instead of a table, so dbt's
`source('raw', 'prometheus_samples')` still resolves.
[test_warehouse_tables.py](../tests/test_warehouse_tables.py) asserts both directions today
(invariant 3). It must honour the flag rather than be loosened.

The nightly `lake_backup` mirror copies raw as well, so the backup bucket grows at the
same rate as the samples. It only ever sees compacted files, apart from the current day.

---

## 5. Models: from samples to questions

The raw stream answers anything, but only for someone who can write counter-reset SQL. The
marts make the common questions typed and cheap. They are built by dbt in
`analytics_daily`, incrementally by `partition_date`, and exposed in the semantic layer
(with the coverage check), `_columns.md` and the catalogue.

| mart | grain | measures (examples) |
|---|---|---|
| `fct_host_metrics_minute` | host × minute | CPU busy ratio, load 1/5/15, memory used/available, swap, root FS used, net rx/tx bps, disk read/write bps, max CPU temperature |
| `fct_container_metrics_5m` | container × 5 min | CPU cores, memory working set, net rx/tx, FS read/write, restarts (start-time changes), OOM events |
| `fct_gpu_metrics_minute` | GPU × minute | utilisation, memory used, temperature, power W, fan, P-state, throttle flags |
| `fct_llm_gateway_minute` | model × minute | requests, failures, p50/p95 latency and time to first token from the histogram buckets |
| `agg_infrastructure_daily` | host or container × day | p95 CPU, max memory, network GB, GPU energy kWh |

A dbt macro `prom_increase(value)` holds the reset rule once, for every counter model.

Questions this enables, joined to what the lake already has:

- Did the GPU saturate when the camera analysis ran, or while Jellyfin was transcoding?
- Which container's memory grew over the month, and did it restart?
- How does gateway-side latency compare with `fct_llm_call.duration_ms` for the same model
  and hour?
- How much GPU energy went to self-hosted model calls on a given day?

---

## 6. What else on this host is not in the lake

The audit listed every other source on the machine -- containers, volumes, Postgres
databases, Kafka topics, buckets -- and ranked them by what each would answer and what
it would cost. That inventory is deployment detail and stays private; what came out of
it is public, in the phases below and in the README's table of sources: Prometheus, the
status page, the media pipeline, the host's own logs, and sign-in events from the
identity server. Two candidates were ruled out rather than deferred -- personal finance,
because the lake is a shared catalogue with no per-domain access control, and video
recordings, which stay out by design.

## 7. Noticed along the way (outside this repo)

- **`ai_platform.usage_events` is [volume withheld] for [count withheld] rows** ([volume withheld] per row: prompt, response
  and results), from July to now. It is already the largest thing in the shared Postgres
  and grows with every call.
- **The Hadoop stack is idle**: 5 containers for 3 months, HDFS holds only `/rmstate`.
  Three k3s/Rancher volumes from Nov 2023 ([volume withheld]) are orphaned. Both can be cleaned up.
- homelab-backend's 2-second CPU/RAM rows ([volume withheld]) duplicate node-exporter and could stop
  or get a retention period.
- Network exposure of the monitoring services was reviewed separately, not here.

---

## 8. Phased plan

### Phase 0: Prometheus hygiene (Prometheus compose, needs your go-ahead)

1. Move the TSDB into a **named volume** (stop, copy, start; about a minute without scrapes).
2. **Pin** `prom/prometheus:v2.48.0`. A 3.x upgrade becomes its own decision.
3. Remove the duplicate jobs, and check the `$job` variable in Node Exporter Full.
4. Fix the AI Platform Overview GPU panel.
5. Optional: retention 15 → 30 days ([volume withheld]). This doubles the repair window of §4.3.

None of this blocks phase 1. Phase 1 reads whatever Prometheus holds.

### Phase 1: capture history (first, because of §0)

`layout` entries plus `lake_only`; the pull client (raw samples, windows, retries); label
privacy; the writer; the backfill job; hourly and daily compaction with their three checks;
series and metadata snapshots; the lifecycle rule; tests (writer round-trip, redaction,
dedupe, hour splitting, coverage both ways).

*Done when:* all 15 days are in the lake, one `data.parquet` per past day, and
`hour_matches_prometheus` passes for every hour.

### Phase 2: the stream

The `prometheus_stream` sensor (option A), cursor recovery, capped ticks with a
`RunRequest` hand-off, the freshness check, and a sensor `retention` block in
[dagster.yaml](../dagster.yaml) so [count withheld] ticks/day do not accumulate.

*Done when:* `select max(ts) from raw.prometheus_samples` is within 2 minutes of now from
the Data workspace. Also: stop the daemon for 20 minutes and start it again, and the hour
still passes `hour_matches_prometheus`.

### Phase 3: models

The five marts of §5, the `prom_increase` macro, semantic-layer entries with coverage,
column docs, and `tests/conftest.py` shapes.

*Done when:* the Datalake Analyst answers "peak GPU power last Tuesday and what was running"
from the marts.

### Phase 4, optional: Kafka push (option C)

Harden Kafka; the remote-write relay; the Dagster consumer with run monitoring on; the pull
path becomes the reconciler that re-pulls any hour whose count falls short; the
`app-metrics` events land through the same consumer.

### Phase 5: the next sources

Each one gets a short plan of its own, in the order you choose from §6.

---

## 9. Decisions needed before phase 1

1. **Streaming mechanism**: option A, the sensor micro-batch (recommended), or go straight
   to C?
2. **What to ingest**: skip the duplicate jobs and Prometheus self-metrics ([volume withheld]/day, host
   and GPU at 30 s), or everything ([volume withheld]/day)?
3. **Retention of raw samples in the lake**: for good (~27–[volume withheld]/year), or a fixed window
   (e.g. 13 months) with the minute marts kept for good?
4. **Domain**: widen `infrastructure`, or add a new `observability` domain?
5. **Phase 0**: may the Prometheus compose on primary be changed (named volume, pin,
   remove duplicate jobs)? In the meantime, should the TSDB blocks be copied aside?
6. **§6**: which sources next, and the privacy calls on Jellyfin playback (16 users) and AI
   platform conversations. Is banking confirmed out?

---

## Status, 2026-09-15

Decisions: option A; every job is ingested (the duplicates were removed at the source
instead); raw samples are kept for good; domain `infrastructure`; phase 0 approved;
privacy handling for the new sources is deferred.

**Phase 0, done on primary.** Prometheus v2.48.0 to v2.55.1 to v3.14.0, on a named volume
(`prometheus-data`), retention 30 days, `restart: unless-stopped`, duplicate jobs removed.
Grafana 10.2.2 to 13.2.1, pinned. The AI Platform Overview GPU panel reads
`nvidia_smi_utilization_gpu_ratio`. The Grafana role mapping returns an empty role instead
of `None` for accounts without `grafana-admin`: `None` is a valid Grafana role, so the old
expression let every realm account in with no role rather than refusing it. The status page
serves Prometheus metrics on an unpublished port and is scraped as job `status-page`.

**Phases 1 and 2, in code.** `assets/prometheus.py`: the `prometheus_stream` sensor, hourly
and daily compaction, metadata snapshots. One change from section 4.2: `count_over_time`
over a whole job fails ("vector cannot contain metrics with the same labelset") because it
drops metric names, so an hour's expected count comes from Prometheus's own per-scrape
record instead (`scrape_samples_post_metric_relabeling` plus five series per scrape), and
missing scrapes are caught exactly by counting `up`. Measured against full raw pulls: exact
for every job except cadvisor, which over-counts by 2.1%. A compacted hour measured 1.14
bytes per sample.

**Section 6 sources, in code.** Radarr, Sonarr and Jellyseerr (`assets/media_pipeline.py`)
are read from their SQLite databases through read-only mounts, not their APIs: no API key
is needed and the databases hold more than the APIs page through. Each table is a ledger of
every distinct row version (`parsing/ledger.py`). Credentials stay out: indexer and client
settings, Jellyseerr passwords and tokens, and API keys inside history URLs. The status page
history, primary's journal and auth.log, and Keycloak sign-in events each have an asset.
Keycloak events need `scripts/setup-keycloak-events.sh` run once by an administrator.

**Not done.** Phase 3 (marts, semantic layer), phase 4 (Kafka), and the AI platform database
and Jellyfin playback, which the platform's phase 41 is landing.
