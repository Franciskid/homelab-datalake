# How the datalake works

Written in ASD-STE100 Simplified Technical English. Short sentences. One idea in each
sentence. Approved words only.

---

## 1. What the system does

The system reads data from five sources in the homelab. It cleans the data. It makes
business tables from the data. It gives the tables to persons and to programs.

The system runs on `primary`. Docker starts it. Dagster controls it.

---

## 2. The three layers

The lake keeps the data in an S3 bucket on MinIO. The bucket has three layers. The
first part of each path is the layer.

| Layer | What it holds | Who writes it |
|---|---|---|
| `raw/` | The data as the source gave it. | The ingest assets |
| `curated/` | The same data, cleaned and with more columns. | `lake_curated` |
| `marts/` | Business tables: facts, dimensions and daily totals. | `lake_marts` |

Do not change a `raw/` object. If you must correct it, run its partition again.

Read `curated/` if you want the records. Read `marts/` if you want the answers.

### Why `raw/` has geo columns

The `raw/` layer has the columns `geo_country_code`, `geo_city` and `geo_asn`. These
columns are derived. Usually a derived column goes in `curated/`.

These columns are different. The system finds the geo data from the client address.
The system then deletes the client address in memory. The system does not write the
address to a disk. Thus the system cannot find the geo data later. To move geo to
`curated/` you must keep the addresses. This is not permitted.

The rule is: `raw/` holds what the source told the system, and what the system could
learn only at that moment. `curated/` holds all other derived data.

---

## 3. The path of an object

Each path has this form:

```
<layer>/<domain>/<source>/<dataset>/<partition>/data.parquet
```

A mart has no source. A mart uses data from more than one source.

```
marts/<domain>/<dataset>/<partition>/data.parquet
```

There are four domains:

| Domain | What it is about |
|---|---|
| `infrastructure` | The reverse proxy and the HTTP traffic |
| `media` | The film library |
| `monitoring` | The cameras |
| `ai` | The model calls and their cost |

Examples:

```
raw/infrastructure/npm/access/host=primary/dt=2026-08-27/data.parquet
raw/media/jellyfin/titles/dt=2026-08-29/data.parquet
raw/monitoring/camera/events/dt=2026-08-15/data.parquet
curated/infrastructure/npm/requests/partition_date=2026-08-27/data.parquet
marts/ai/agg_llm_spend_daily/partition_date=2026-08-27/data.parquet
```

The file `src/homelab_datalake/layout.py` holds all the paths. Do not write a path
in any other file. If you move a dataset, change `layout.py` and run
`scripts/migrate-lake-layout.sh`.

---

## 4. The datasets

### raw

| Path | Rows | Partition |
|---|---|---|
| `raw/infrastructure/npm/access` | One HTTP request | `host`, `dt` |
| `raw/infrastructure/npm/access_rejects` | One log line that no format matched | `host`, `dt` |
| `raw/ai/litellm/spend` | One model call | `dt` |
| `raw/media/jellyfin/titles` | One film, each day | `dt` |
| `raw/media/jellyfin/title_genres` | One film and one genre | `dt` |
| `raw/media/tmdb/titles` | One TMDB record | none |
| `raw/monitoring/camera/events` | One camera event | `dt` |
| `raw/monitoring/camera/clips` | One video clip record | `dt` |
| `raw/monitoring/camera/logs` | One activity log entry | `dt` |

### curated

| Path | dbt model | What the layer adds |
|---|---|---|
| `curated/infrastructure/npm/requests` | `int_npm_requests` | Country, city, network operator, bot or browser, path group |
| `curated/monitoring/camera/events` | `int_camera_events` | Late analysis applied, duplicates marked, duration |
| `curated/ai/litellm/spend` | `stg_litellm_spend` | Workload, provider, self-hosted flag |
| `curated/media/jellyfin/titles` | `stg_jellyfin_titles` | Correct types, decade, resolution class |
| `curated/media/jellyfin/title_genres` | `stg_jellyfin_title_genres` | Correct types, no duplicates |
| `curated/media/tmdb/titles` | `stg_tmdb_titles` | Correct types, revenue to budget ratio |

### marts

| Path | dbt model | Grain |
|---|---|---|
| `marts/infrastructure/fct_http_request` | `fct_http_request` | One HTTP request |
| `marts/infrastructure/agg_traffic_daily` | `agg_traffic_daily` | Day, host, client kind |
| `marts/ai/fct_llm_call` | `fct_llm_call` | One model call |
| `marts/ai/agg_llm_spend_daily` | `agg_llm_spend_daily` | Day, model, provider, workload |
| `marts/monitoring/fct_camera_event` | `fct_camera_event` | One camera event |
| `marts/monitoring/agg_camera_activity_daily` | `agg_camera_activity_daily` | Day, camera, zone, rule |
| `marts/media/fct_library_title` | `fct_library_title` | One film, each day |
| `marts/media/dim_title_scd2` | `dim_title_scd2` | One version of one film |
| `marts/media/dim_genre` | `dim_genre` | One genre |
| `marts/media/bridge_title_genre` | `bridge_title_genre` | One film and one genre |

---

## 5. How the data moves

```
source  ->  raw/ (Parquet)  ->  DuckDB raw.*  ->  dbt  ->  curated/ and marts/ (Parquet)
                                                     |
                                                     +->  serving.duckdb  ->  the MCP server
```

1. An ingest asset reads a source. It writes one Parquet object for each partition.
2. `warehouse_tables` reads all the Parquet. It makes the DuckDB tables `raw.*`.
3. dbt makes the staging, intermediate and mart models in DuckDB.
4. `lake_curated` and `lake_marts` write those models back to the lake as Parquet.
5. `warehouse_serving` copies the marts to `serving.duckdb`.

Step 5 is necessary. DuckDB permits one writer only. A reader of the build file gets
an error while the build runs. The copy lets readers work at all times.

### The Dagster assets

The files are in `src/homelab_datalake/assets/`.

| File | Asset | What it writes |
|---|---|---|
| `npm_access.py` | `npm_access_raw` | `raw/infrastructure/npm/access` and `.../access_rejects` |
| `litellm_spend.py` | `litellm_spend_raw` | `raw/ai/litellm/spend` |
| `jellyfin.py` | `jellyfin_library_raw` | `raw/media/jellyfin/*` |
| `tmdb.py` | `tmdb_titles_raw` | `raw/media/tmdb/titles` |
| `camera.py` | `camera_events_raw` | `raw/monitoring/camera/*` |
| `warehouse.py` | `warehouse_tables` | The DuckDB tables `raw.*` |
| `dbt_marts.py` | `dbt_marts` | The DuckDB staging, intermediate and mart models |
| `publish.py` | `lake_curated`, `lake_marts` | `curated/*` and `marts/*` in the lake |
| `warehouse.py` | `warehouse_serving` | `serving.duckdb` |

The job `analytics_daily` runs steps 2 to 5. It runs at 04:15 UTC each day.

### How a publish replaces

`lake_curated` and `lake_marts` write all the partitions first. Then they delete the
objects that this run did not write. The prefix is never empty. A reader that reads
during a rebuild gets the old data or the new data. The reader never gets nothing.

---

## 6. Privacy

The system never writes a client IP address. The file
`src/homelab_datalake/privacy.py` is the only place an address exists. The address
stays in memory. The system writes only:

* `client_hash`: a salted hash of the address.
* `client_network`: the address made coarse (/24 or /48).
* `client_trust`: `resolved`, `cdn_edge` or `unknown`.
* The geo columns.

A dbt test checks this rule. The test is
`dbt/tests/assert_no_raw_ip_addresses.sql`. The build fails if an address is found.

---

## 7. Commands

Run these from a clone of this repository on your own computer.

Deploy the code and rebuild the marts:

```bash
bash scripts/deploy.sh
```

Deploy only. Do not rebuild:

```bash
bash scripts/deploy.sh --no-run
```

Show what the lake migration would move:

```bash
bash scripts/migrate-lake-layout.sh
```

Move the objects:

```bash
bash scripts/migrate-lake-layout.sh --apply
```

Run the tests:

```bash
.venv/bin/python -m pytest -q
```

---

## 8. If something fails

| Symptom | Cause | What to do |
|---|---|---|
| A dbt model is not found | The image has old code. | Run `scripts/deploy.sh`. It compares checksums. |
| `Conflicting lock is held` | A step opened DuckDB to write. | Only the build writes. Use `read_only_warehouse()`. |
| `Catalog "datalake" does not exist` | DuckDB attached the file with the wrong name. | A view keeps the catalogue name. Attach with the file name. |
| A run failed and nobody knew | `PLATFORM_ALERT_URL` or `PLATFORM_ALERT_TOKEN` is empty, so the sensor logs and delivers nothing. | Set both in `.env`; the token is the platform's `ALERTMANAGER_WEBHOOK_TOKEN`. Failures then open an incident, deduplicated per job and partition. |
