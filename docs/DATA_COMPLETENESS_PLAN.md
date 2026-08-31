# Data completeness: audit and plan

Written 2026-08-31. Nothing here is implemented yet.

The brief: the lake must keep everything each source offers, an event must carry its
associated clip and its own metadata, corrections must survive into the raw layer while
the marts present the corrected truth with an audit trail, and the camera history must be
re-ingested so old partitions gain the fields they never had.

Every number below was measured against production on 2026-08-31, not inferred.

---

## 0. Blocker: re-ingesting as the asset stands today destroys [count withheld] events

`camera_events_raw` replaces a partition — `lake.delete_prefix(prefix)`, then write
whatever the export API returns. That is correct while the camera still holds the day.
It no longer does.

The camera's `attention-events.json` is hard-capped at `maxEvents = 5000` and currently
[measurement withheld]. Events past the cap are evicted at source, except confirmed state
events which survive 90 days (max 1000). The lake has been running longer than that
window is deep, so the lake is now the *only* copy of part of August.

Measured, every partition, lake versus what the API returns today:

| day | in lake | API returns now | lost by a replace |
|---|---|---|---|
| [date withheld] | [withheld] | [withheld] | **[withheld]** |
| all other days | [withheld] | [withheld] | [withheld] |
| **total** | **[withheld]** | **[withheld]** | **[measurement withheld] ([rate withheld])** |

A `dagster job backfill` over the camera partitions, with the asset as written, would
permanently delete [rate withheld] of the raw layer. The plan below therefore makes the raw asset
merge-on-write **before** any backfill runs. This is step 1 and nothing else may go first.

---

## 1. Why `counts` was dropped

Not a bug in the mechanism — an omission in a list.

`assets/camera.py` projects every row through `_project()`, which keeps exactly the
columns named in `_EVENT_SCHEMA` and silently discards the rest. That is deliberate, and
the reasoning in the docstring is sound: it stops an upstream change from widening the
warehouse without anyone reviewing it, including a change that starts sending something
nobody meant to store.

The flaw is the failure mode, not the intent. Deny-by-default with **no signal** means a
field the source adds is indistinguishable from a field that never existed. `counts` has
been arriving on every event since before the pipeline started; it was simply never added
to the schema, and nothing anywhere said so.

The fix is not to delete `_project()`. It is to keep the explicit schema *and* stop the
silence — see §3.

---

## 2. Audit: what each source drops

### 2.1 Camera events

Dropped by `_project()` (the export sends it, the lake discards it):

| field | populated | why it matters |
|---|---|---|
| `counts` `{person,vehicle,animal,other}` | [measurement withheld] [count withheld] events, 7–31 Aug | **every bar of the alerts graph is a sum over this.** Totals: [measurement withheld] |

Dropped later in the chain:

- `ruleId` — lands in raw, dropped at `stg_camera_events.sql`
- `state_observed_at` — survives raw → staging → intermediate, dropped at
  `fct_camera_event.sql`. For a gate transition this is when the state was *observed*

Never exported by the camera at all (`export-routes.js` maps a fixed field list). Present
in the camera's store, unreachable from the lake:

| field | non-null in store | note |
|---|---|---|
| `occurredAt` | [withheld] | the camera's own report ranks and filters on `occurredAt \|\| startedAt`; the lake only ever sees `startedAt` |
| `correction` | [withheld] | `{previous:{…11 fields}, correctedAt, correctedBy:{id,name,email}}` |
| `transitionClipId` | [withheld] | the clip that filmed the gate actually moving |
| `stateSource` | [withheld] | |
| `backfill` | [withheld] | `{at, reason}` |
| `repair` | [withheld] | `{incident, reconciledAt}` |
| `restoration` | [withheld] | `{restoredAt, source}` |
| `timingCorrection` | [withheld] | `{previous:{occurredAt,stateObservedAt}, evidence, source}` |

Also: `export-routes.js` carries the comment *"zoneRevision travels; `shapes` never
does"* — `zoneRevision` is not in the mapped object. Stale comment, or a field that was
meant to ship and doesn't.

### 2.2 Camera clips and logs — landed, then abandoned

`raw.camera_clips` ([count withheld] rows) and `raw.camera_logs` ([count withheld] rows) are ingested, declared
and fully documented in `_sources.yml` — and then have **no staging model and no mart**.
Only `camera_events` gets a staging → intermediate → mart chain.

This is why "an event must carry its associated clip" is currently impossible from the
marts. The join is real and clean: `fct_camera_event.clip_id → raw.camera_clips.id`
matches **[measurement withheld] distinct clip ids, [rate withheld]**. It is one model away.

Dropped by `_project()` on clips: `title` (present on [measurement withheld] sampled, e.g. `"1 personne"`).

Logs deliberately withhold their free-text `title` at the camera, because on this
deployment it contains @mentions. That is a good call and should stay.

### 2.3 Camera attention zones — no endpoint exists

Not exported at all. There is no `/api/export/zones`. `attention-zones.json` ([volume withheld]) holds
the zone configuration the report needs to know which zones are gates.

Two further camera-side datasets that the alerts graph depends on and that nothing exports:

- `attention-observability.json` — per-zone unknown/blind spans, keyed `{cameras, zones}`.
  The gate ribbon cannot be drawn without these; without them a blind stretch is
  indistinguishable from a closed gate.
- `annotations.json` — [measurement withheld], each with `metadata {zoneId, ruleId, eventType}`,
  `timestampMs`, `createdAt`, `scope`, `targetId`.

### 2.4 Jellyfin

`_title_row` keeps one value where the API returns a list, and `FIELDS` never asks for
several things Jellyfin already holds. Measured over a [measurement withheld]:

Truncated to `[0]`:
- `Studios` — one title in the sample has **25** studios; the lake keeps the first
- `ProductionLocations` — `['United Kingdom', 'Japan']` becomes `'United Kingdom'`
- `ProviderIds` — keeps `Tmdb` + `Imdb`, drops `TmdbCollection` (franchise membership)
- `MediaStreams` — keeps first Video + first Audio. One sampled title carries **34
  subtitle streams**, all discarded, along with every extra audio language, and each
  stream's `Language`, `DisplayTitle`, `IsDefault`, `IsForced`

Returned by the API, never stored:

| field | present | field | present |
|---|---|---|---|
| `People` (cast/crew) | [withheld] | `Tags` | [withheld] |
| `MediaSources` | [withheld] | `RemoteTrailers` | [withheld] |
| `ExternalUrls` | [withheld] | `Taglines` | [withheld] |
| `SortName` | [withheld] | `Chapters` | [withheld] |
| `GenreItems`, `Width`, `Height`, `ParentId`, `VideoType`, `MediaType` | [withheld] | `HasSubtitles` | [withheld] |

Deliberately excluded and worth keeping excluded: `Path` — it encodes filesystem topology
and the name of the site each file came from.

`Overview` is currently excluded as "prose no question asks about". Under a keep-everything
rule it should land.

One correctness note found in passing: `resolution_class` disagrees with Jellyfin itself.
A title whose video stream has `Height: 800` and `DisplayTitle: "1080p H264 SDR"` is
classified `720p` by `_RESOLUTION_BANDS`. Landing `DisplayTitle` gives the source's own
answer alongside the derived one.

### 2.5 LiteLLM

`LiteLLM_SpendLogs` has 33 columns; `_SCHEMA` keeps 24. Dropped, with population measured
over all [count withheld] rows:

| column | populated | contents |
|---|---|---|
| `metadata` (jsonb) | **[withheld]** | see below |
| `cache_key` | [withheld] | |
| `model_id` | [withheld] | |
| `request_tags` (jsonb) | [withheld] | |
| `api_base` | [withheld] | |
| `user` | [withheld] | |
| `organization_id`, `agent_id` | [withheld] | empty on this deployment |

`metadata` is the significant one. It is populated on every single row and contains
figures the lake has no equivalent of:

- `usage_object.completion_tokens_details.reasoning_tokens`
- `usage_object.prompt_tokens_details.cached_tokens`
- `cost_breakdown.{input_cost, output_cost, original_cost, discount_amount, margin_*}` —
  the lake has only a single `spend_usd`
- `applied_guardrails`, `error_information`, `attempted_retries`, `max_retries`
- `model_map_information.model_map_key`, `user_api_key_alias`

Needs a decision (§7): `messages`, `response`, `proxy_server_request` are the prompt and
completion payloads.

### 2.6 NPM access logs

- `referer` — parsed from every line, carried on `ParsedLine`, and **absent from
  `_SCHEMA`**. Captured and then thrown away at the write.
- `gzip` ratio — captured by both the `_PROXY` and `_STANDARD` regexes, never even
  reaches `ParsedLine`.
- query string — `extract.py:156` does `"path": strip_query(parsed.uri)`. Deliberate:
  query strings here carry reset tokens and API keys. Keep stripping the values; see §7.

### 2.7 TMDB

`_SCHEMA` keeps 5 enrichment fields. The endpoint returns 27. Dropped:

`vote_average` (the lake leans on Jellyfin's `community_rating` as a proxy for it),
`belongs_to_collection`, `genres`, `production_companies`, `production_countries`,
`spoken_languages`, `origin_country`, `runtime`, `status`, `tagline`, `release_date`,
`homepage`, `adult`, `video`, `overview`, `poster_path`, `backdrop_path`, `imdb_id`,
`original_title`, `title`, `id`.

---

## 3. Design: stay explicit, stop being silent

Keep `_project()`. Change what it does with what it doesn't recognise.

1. **Widen every declared schema** to the full field set each source sends, using real
   Arrow types. Nested things become `pa.struct`, not JSON strings — DuckDB reads Parquet
   structs natively, so `counts.person` and `correction.correctedBy.name` work in plain
   SQL with no unpacking. Repeated things become `pa.list_` (studios, production
   locations, media streams, request tags).

2. **Add `_unmapped` to every raw schema** — a JSON column holding any key the source sent
   that the declared schema does not name. Nothing can be lost again; the projection stays
   explicit and reviewable; a new upstream field lands as data on day one.

3. **Add an asset check per source: `no_unmapped_fields`.** Warns when `_unmapped` is
   non-empty on any row, naming the keys. This is the signal whose absence caused this
   whole problem — it turns "a field appeared and vanished" into a warning with the field
   name in it.

4. **Surface everything downstream.** Staging renames and casts but drops nothing. Marts
   may add derived columns; they may not remove source ones. Add a dbt test asserting that
   every raw column reaches its mart, so the `state_observed_at` class of loss cannot recur.

---

## 4. Corrections: raw keeps both accounts, the mart resolves

Requested shape, and it is the right one.

**Raw** lands the event exactly as the source describes it, corrections included:
`correction.previous.*` (the machine's original account), `correction.correctedAt`,
`correction.correctedBy.{id,name}`, alongside the current corrected values. Nothing is
resolved here — raw is evidence.

**Staging/intermediate** pick the surviving version per event id, as they do now.

**Marts** present the corrected value as the truth, and carry the provenance beside it:

| column | meaning |
|---|---|
| `was_corrected` | boolean |
| `corrected_at` | when a person overrode the machine |
| `corrected_by_name` / `corrected_by_id` | who |
| `machine_<field>` | the pre-correction value, per correctable field |
| `correction_source` | `human`, `timing`, `repair`, `restoration`, `backfill` |

`CORRECTABLE_FIELDS` on the camera is the authoritative list: `lifecycle`, `direction`,
`counts`, `state`, `previousState`, `stateObservedAt`, `dismissalReason`, `duplicateOf`,
`summary`, `confidence`, `shouldNotify`.

`correctedBy.email` is present on [measurement withheld]. Hash it with the existing identity
salt rather than landing it — the name already answers "who".

---

## 5. Phased plan

### Phase 1 — make re-ingestion non-destructive (must be first)

- `camera_events_raw` merges instead of replacing: read the existing partition, union
  with the API response keyed on event id, keep the newest version per id by
  `_extracted_at` with `lifecycle <> 'candidate'` breaking ties. This is the rule
  `int_camera_events` already implements; it moves up to the write.
- New asset check `partition_did_not_shrink` — fails when a re-materialised partition
  would contain fewer distinct event ids than it did before. `jellyfin_library_raw`
  already has exactly this guard (`MAX_SHRINK_RATIO`); camera has none.
- Take a copy of `raw/media/camera/` before anything else runs. `scripts/` already has
  the backup and restore path; this is one invocation of it.

**Exit test:** re-materialise 2026-08-08 and assert the partition still holds [count withheld] events,
[measurement withheld].

### Phase 2 — widen the raw schemas

Per source, in this order: camera events, camera clips, camera logs, Jellyfin, LiteLLM,
NPM, TMDB. Each is: extend `_SCHEMA`, extend the row builder, add `_unmapped`, add the
`no_unmapped_fields` check, extend `_sources.yml` with a description per new column (the
repo's `assert_every_column_is_documented` test enforces this).

### Phase 3 — camera-side exports

Needs a change to `~/projects/camera/camera-app` on secondary, deployed there.

- Add `occurredAt`, `transitionClipId`, `correction`, `stateSource`, `backfill`, `repair`,
  `restoration`, `timingCorrection`, `zoneRevision` to the `/api/export/events` map, and
  `title` to `/api/export/clips`.
- New `GET /api/export/zones` — zone configuration. Geometry decision in §7.
- New `GET /api/export/observability` — the unknown/blind spans.
- New `GET /api/export/annotations`.
- All four follow the existing contract: half-open `[from, to)`, ordered by timestamp then
  id, NDJSON, streamed, `requireExportToken`.

Then three new Dagster assets — `camera_zones_raw`, `camera_observability_raw`,
`camera_annotations_raw` — registered in `layout.py` (the only place a path is decided).

### Phase 4 — backfill

Only after phases 1–3. Re-materialise camera partitions 2026-08-07 → today.

State plainly what this can and cannot do: it fills the new columns for every event the
camera **still holds**. The [count withheld] events on 2026-08-08 that only the lake has will keep null
`counts` forever — the source cannot answer for them any more. Nothing recovers July;
ingestion began 2026-08-07 and the camera's window has long since passed it.

### Phase 5 — models

- `stg_camera_clips`, `stg_camera_logs`, `stg_camera_zones` — the missing staging layer.
- `fct_camera_clip`, `dim_camera_zone` marts.
- `fct_camera_event` gains `state_observed_at`, `rule_id`, `occurred_at`, the `counts.*`
  columns, the correction block from §4, and the clip join.
- Declare the foreign keys (§6) — as `relationships` tests, so they are enforced and not
  merely documented.

### Phase 6 — the export path

Separate from data completeness but it is what the user actually sees, and without it
none of the above reaches an Excel file:

- Raise or paginate `MAX_ROWS = 1000` (`api/query.py:61`, clamped again at
  `api/app.py:160` and `apps/api/src/routes/datalake.ts:22`).
- Make `datalake.query` surface `truncated` and `notes` instead of discarding them
  (`apps/python-sandbox/src/runner.ts:453`). The API already returns both.

---

## 6. Feasibility: a relations export so the LLM stops guessing joins

**Yes. Most of it already exists and is not being served.**

### What is already there

`dbt/target/manifest.json` is built at image build time and is present in
`datalake-mcp`, `datalake-api` and `datalake-daemon`. `api/catalogue.py` already loads it
for column descriptions. Nothing new has to be generated or scheduled.

Measured in the current manifest (dbt 1.11.14, schema v12):

- 18 models, 9 sources, 114 tests
- **Model lineage is complete** — `depends_on.nodes` on every model, plus `parent_map` and
  `child_map` for the whole DAG
- **Column documentation is [rate withheld]** — all 283 columns across all 18 models carry a
  description. This is the expensive part of a semantic export and it is already done
- **dbt already infers a primary key on 7 of 18 models** from paired `unique` + `not_null`
  tests: `fct_camera_event.event_id`, `fct_library_title.title_key`,
  `fct_llm_call.call_key`, `dim_title_scd2.title_version_key`, `dim_genre.genre_key`,
  `stg_litellm_spend.request_id`, `stg_tmdb_titles.tmdb_id`

### What is missing

**Foreign keys: 2 declared, out of an actual graph of at least 6.** Only
`bridge_title_genre → fct_library_title` and `bridge_title_genre → dim_genre` exist.

The other edges are real and clean. Measured referential integrity in production:

| edge | distinct left | matched right | integrity |
|---|---|---|---|
| `bridge_title_genre.title_key → fct_library_title.title_key` | [withheld] | [withheld] | [rate withheld] |
| `bridge_title_genre.genre_key → dim_genre.genre_key` | [withheld] | [withheld] | [rate withheld] |
| `dim_title_scd2.item_id → fct_library_title.item_id` | [withheld] | [withheld] | [rate withheld] |
| `fct_camera_event.event_id → raw.camera_events.id` | [withheld] | [withheld] | [rate withheld] |
| `fct_camera_event.clip_id → raw.camera_clips.id` | [withheld] | [withheld] | [rate withheld] |
| `fct_camera_event.camera_id → raw.camera_clips.cameraId` | [withheld] | [withheld] | [rate withheld] |

Every one of these joins works today. None of them is written down anywhere a model can
read.

**And the tooling gives an LLM no way to use them.** There are two paths and neither
helps:

- `mcp/semantic.py` is a single-dataset design by deliberate choice — dataset, grain,
  dimensions, measures, filters. It has no concept of a join. Its docstring argues the
  case explicitly, and the argument is good, but the consequence is that any
  cross-dataset question is unanswerable through it.
- The SQL path (`/sql`, `datalake.query`, the MCP query tool) takes free text against a
  schema the model has to infer from names. That is exactly the retry loop described.

### Proposed design

1. **Declare the edges in dbt YAML as `relationships` tests.** This is the key move: one
   declaration produces both the documentation *and* an enforced test, so the graph
   cannot silently rot the way `state_observed_at` did. dbt writes them into the manifest
   as `test_metadata.kwargs.{to, field}` with `attached_node` and `column_name` — a
   machine-readable FK, for free.

2. **Add `GET /catalogue/relations`** to `api/catalogue.py`, which already parses the
   manifest and already caches on the publish stamp. It emits, per table: primary key,
   foreign keys with their target table and column, model lineage, and the existing
   column descriptions. A compact JSON join graph.

3. **Add an MCP tool `describe_schema`** returning that graph, so an agent fetches the
   relations once at the start of a task instead of guessing and retrying. Register it in
   `packages/agent-runtime/src/mcp-profiles.ts` — both the `READ_ONLY` policy array and
   the `TOOL_METADATA` map, or it can neither be called nor discovered.

4. **Optional, and worth it later:** a profiling pass that measures overlap between
   candidate key columns and publishes edges above a threshold as `inferred` rather than
   `declared`. The table above is that pass, run by hand. It would catch the next
   undeclared join without waiting for someone to notice.

### Cost and risk

Small. Steps 1–3 are roughly a day: the FK declarations are a dozen YAML entries, the
endpoint is a traversal of a file already in memory, the MCP tool is a wrapper plus two
profile registrations. No new infrastructure, no new schedule, no new storage.

The one risk worth naming: declaring a `relationships` test makes it **run**, and an edge
that is not actually [rate withheld] clean will start failing the nightly build. The table above was
measured precisely so that surprise does not happen — all six are clean today. Any edge
added later should be measured before it is declared.

---

## 7. Four decisions needed before phase 2

1. **Zone geometry.** `export-routes.js` withholds polygon `shapes` on the argument that
   they are a map of the property, and the repo makes that case at length. Zone metadata
   without geometry — id, name, cameraId, rules, enabled, thresholds, revision — is safe
   and is what the report needs. Recommend exporting the metadata, leaving `shapes`
   behind, unless you want the geometry too.
2. **LiteLLM `messages` / `response` / `proxy_server_request`.** Full prompt and
   completion payloads on [count withheld] calls. Everything else in §2.5 is analytics and should
   land. Recommend these three stay out, or land under a separate restricted prefix that
   the SQL console and the agents cannot read.
3. **Jellyfin `Path`.** Recommend it stays excluded — it carries the site name each file
   came from.
4. **NPM query strings.** Recommend the values stay stripped, and that we land
   `has_query` plus the sorted *parameter names* instead. That answers "which endpoints
   take parameters" without storing a single token.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
