# How the datalake works

Written in ASD-STE100 Simplified Technical English. Short sentences. One idea in each
sentence. Approved words only.

---

## 1. What the system does

The system reads data from five sources in the homelab. It cleans the data. It makes
business tables from the data. It gives the tables to persons and to programs.

The system runs on `fraserv`. Docker starts it. Dagster controls it.

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
raw/infrastructure/npm/access/host=fraserv/dt=2026-08-27/data.parquet
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
| `raw/infrastructure/infra_control/edge_decisions` | One edge block or never-block (a full snapshot each day) | `dt` |
| `raw/infrastructure/infra_control/settings` | One global setting and its value | `dt` |
| `raw/infrastructure/infra_control/config_changes` | One change to a global setting | `dt` |
| `raw/monitoring/camera/events` | One camera event | `dt` |
| `raw/monitoring/camera/clips` | One video clip record | `dt` |
| `raw/monitoring/camera/logs` | One activity log entry | `dt` |
| `raw/official_statistics/publishers/observations` | One version of one official figure (Eurostat, SSMSI, State budget, DREES) | none |

The three `infra_control` tables come from fraserv-infra's infra-control (its export
routes, the lake's own client token), which owns the NPM blocklist and the installation's
global settings since its cutover; see its `docs/control/`. `fct_edge_decision` reads them
first and the AI platform's frozen `edge_blocks` only for decisions infra-control never saw;
`fct_infra_config_change` answers who changed which setting, through what, and why.

`official_stat_observations` holds the Eurostat series and the SSMSI's recorded crime that
politico.fraserv.fr charts. It is a ledger keyed by publisher, dataset, series, place and
period, so a publisher's revision is a new version and the earlier figure stays. The same run
writes `official-statistics.json` into the directory shared with the politico stack
(`OFFICIAL_STATS_EXPORT_HOST_DIR`, default `/home/francois/dev/politico-data`), daily at
05:40 UTC. A source that fails that morning keeps its previous block in the file, marked
`stale`. `fct_official_stat_observation` is the current value of each figure with its
revision count.

### Poll ingestion (`polls_ingestion`)

politico.fraserv.fr's observatory shows the voting-intention polls of every institute. Each morning at 06:10 UTC (after the
official statistics) `polls_ingestion` looks for new reports and writes `polls-export.json` into the same directory as
`official-statistics.json`. The site fetches that file when it loads, so a new poll appears without a redeploy.

1. **Discovery.** The Commission des sondages notices list (every "Pres IV" notice of the last 45 days), the publication pages
   of the institutes (Elabe, Ifop, OpinionWay, Harris/Toluna, Cluster17, Ipsos, Odoxa, Verian, YouGov, BVA: listing, then article,
   then PDF) and the French Wikipedia table of polls, which has no document and so is only a cross-check: a poll it lists that
   no report matched is reported as `wikipedia_only`.
2. **Download and read.** Only https, 60 MB at most, and only through `pdftotext` (poppler-utils in the image). Elabe's full
   report has its own parser; every other report goes through the generic table reader. Both are the code of the politico
   repository's `scripts/pollslib`, **vendored** into `src/homelab_datalake/parsing/polls/` by politico's
   `scripts/vendor-pollslib.py` (the copy carries a header and is not edited here).
3. **Validation.** ISO dates with a fieldwork of at most 20 days, sample size, https source, a page for every hypothesis,
   known candidate ids, scores summing to 100 ± 2 (a duel has exactly two candidates), cross-tab totals equal to the published
   scores, no duplicate (same id, or same institute and fieldwork dates, or the same figures a day apart).
4. **Verdict.** `auto`: every check passes and the reader is trusted (Elabe's full report; Ifop, OpinionWay and YouGov through
   the generic reader, measured against 86 hand-checked reports). Published polls carry `reviewed: false` and
   `ingest.mode: "auto"`; the poll popup says "relevé automatique, non relu". Everything else is `review`: never published,
   listed in `review` with its reasons (`scripts/polls-review.py list` in the politico repository).
5. **Memory.** The previous export is the job's state: a report already read is not downloaded again; a review item stays until
   the site bundles that poll.

The export holds only what is new compared with the polls the site bundles, which the job reads from
`https://politico.fraserv.fr/polls-index.json` (override `POLLS_INDEX_URL`; with the site unreachable it falls back to a
`polls-index.json` next to the export, and without either it refuses to run rather than ingest blind). Its shape is documented
in the politico repository's `docs/polls-data-schema.md`: polls, plus cross-tabs, certainty of choice, camp-leadership and other
questions. A source that fails (or whose page changed) is reported in `job.sources` and the check `sources_answered` warns; the
others still run.

Environment: `POLLS_EXPORT_DIR` (default `OFFICIAL_STATS_EXPORT_DIR`, else `/exports/politico`), `POLLS_INDEX_URL`.

For politico.fraserv.fr's "Dépenses publiques" the same run also reads: French general-government
expenditure by COFOG function (Eurostat `gov_10a_exp`, all 80 codes, 1995 onward, total /
compensation of employees / social benefits, by sub-sector), the social contributions received
(`gov_10a_taxag`, actual and imputed employers' ones), employers' contributions and pay of the
whole government (`nasa_10_nf_tr`), GDP in volume (`nama_10_gdp`); the State budget's personnel
categories by programme (data.economie.gouv.fr, editions 2019, 2023, 2024, 2025); the CAS
Pensions revenue lines; and the DREES "Comptes de la protection sociale" for old age and
survivors by scheme (1959 onward). They are landed as raw inputs only: how pension
contributions are reallocated to functions is an assumption that lives with the method in the
renderer (`donnees/`, politico repository), so changing it never means re-landing history.
Pension pages add: the COR's June report data files (observed series and reference scenario of resources, contributors,
retirees, average pay and pension; read with a stdlib xlsx reader because some COR workbooks defeat openpyxl), the executed
CAS Pensions and its contribution rates (Service des retraites de l'État page), the DREES long series of resources of social
protection (workbook found through data.gouv.fr), employment and the tax wedge (Eurostat), and curated tables read by hand
from PDFs with their page numbers (`parsing/curated_pensions.py`: PAP contributions to the CAS Pensions per programme, CCSS
employer contributions of the CNRACL and the Ircantec). `scripts/export_official_statistics_local.py` runs the same
collectors on a laptop.

Phase 7b adds (all public, no key): Insee workbooks on immigrants and descendants of immigrants (stock since 1911, origin, age,
employment, unemployment, diplomas, living standards, housing), on the Maastricht debt, deficit and interest, and on suspects by
nationality, each kept *as a grid* (one row per numeric cell, the text of every text cell as labels), so the renderer picks cells by
their labels and a changed layout withholds a page instead of publishing a wrong cell; Insee's consumer price index (annual mean of
the twelve months); the OECD tax wedge (average wage and 67 % of it, by country); the Banque de France's share of the State's
negotiable debt held by non-residents (through DBnomics); the SSMSI's regional base; the Ministry of Justice's latest monthly prison
workbook; and two more Eurostat queries (`gov_10a_main` revenue and interest, annual 10-year yields). The CNRACL employer rate is
known year by year from 2013 and four more State missions are in the curated PAP table. The workbook URLs are edition-specific
(Insee page ids change each year): `parsing/deeper_series.py` lists them in one place.

Debt history (round 7c): two more Insee workbooks, kept as grids like the others. `insee_dette_longue` (page 2830192) is the
Maastricht debt of the general government by sub-sector, **1978 to the latest year**, in billions of euros and as a share of GDP
(national accounts base 2020; Insee recomputes the whole series at constant definition, so there is no break inside it).
`insee_population_longue` (bilan démographique, tableaux rétrospectifs, page 8721185, file POP1) gives the population at 1 January:
France since 1982 and France métropolitaine since 1901 (the two tabs `FR` and `FM`), which is what turns the debt into a figure per
head before 1994. The two populations do not cover the same territory (about 2.3 % apart in 1982): the renderer says where it
switches. Consumer prices stay from 1990: Insee's BDM index has no earlier annual series.

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
| `marts/official_statistics/fct_official_stat_observation` | `fct_official_stat_observation` | One official figure, at its current value |

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
| `official_statistics.py` | `official_statistics_raw` | `raw/official_statistics/publishers/observations`, and the politico export |
| `polls_ingestion.py` | `polls_ingestion` | `polls-export.json` in the politico export directory (no lake table) |
| `warehouse.py` | `warehouse_tables` | The DuckDB tables `raw.*` |
| `dbt_marts.py` | `dbt_marts` | The DuckDB staging, intermediate and mart models |
| `publish.py` | `lake_curated`, `lake_marts` | `curated/*` and `marts/*` in the lake |
| `warehouse.py` | `warehouse_serving` | `serving.duckdb` |

The job `analytics_hourly` runs steps 2 to 5, every hour at
:55 UTC, after the last hourly source lands.

### What an hourly run redoes

A raw table is reloaded only if its objects in the lake changed. dbt builds only what
is downstream of those tables. The serving copy is replaced only if a mart was rebuilt.
Nearly all of the warehouse lands once a night, so this spares most of the hourly write.

Everything is still rebuilt on the first run from 03:00 UTC each day and after any
change to the code. To force it:

```bash
docker exec -e DAGSTER_HOME=/dagster_home datalake-daemon dagster job execute -j analytics_hourly -m homelab_datalake.definitions --tags '{"datalake/full_rebuild": "true"}'
```

The state lives in the `build_state` schema of the warehouse. See
`src/homelab_datalake/rebuild_state.py`.

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
