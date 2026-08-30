# Plan: the data workbench

Written in ASD-STE100 Simplified Technical English. Short sentences. One idea in each
sentence.

This plan is not started. A different session can execute it from this file.

Related: [HOW_IT_WORKS.md](HOW_IT_WORKS.md) tells you how the lake works now.

---

## 1. What to build

A new tab in `ai.example.test`, at **Explore > Data**. The tab does four things:

1. It shows every table in the lake, with the columns, the types and the descriptions.
2. It runs SQL. A switch controls if the SQL can write.
3. It runs Python on the result, in a sandbox. The Python can make Excel and Word files.
4. It saves a Python script, so a model can run the script again later.

A second new tab, **Explore > Scripts**, holds the saved scripts.

Two repositories change:

* `homelab-datalake` gets an HTTP API. All the safety controls are in it.
* `ai-platform` gets the two tabs, the sandbox changes and the script store.

---

## 2. Decisions that are made

The user made these decisions on 30 August 2026. Do not change them.

| Decision | Answer |
|---|---|
| Python sandbox | Give it pandas, openpyxl and python-docx. It must read all the data. |
| SQL editor | Use CodeMirror 6 with `@codemirror/lang-sql`. |
| Saved scripts | Keep them in `ai-platform`, in a new tab. |
| Writes to the lake | Permit them. Add daily backups that recover the data. |

---

## 3. Facts you must know first

These were measured on the running systems. They control how the work is done.

**The sandbox network has no exit.** `docker network inspect
primary-ai-platform_sandbox` reports `internal=true`. Only two containers are on it:
the sandbox and the API. The sandbox cannot reach MinIO, the internet, or the
datalake. **Do not attach the sandbox to another network.** Send its data requests
through the API, which is already its neighbour.

**The sandbox image is Alpine.** Alpine uses musl. pandas has no musl wheel, so pip
compiles it, which needs a build toolchain and much time. Change the runtime stage to
`python:3.12-slim`, which is Debian and has wheels.

**The sandbox takes JSON, not a connection.** Today the caller sends `dataset` as a
JSON array. The limit is [volume withheld]. [count withheld] rows are near [volume withheld]. Give the sandbox a
`datalake` helper instead. The helper sends the SQL to the API, and the API sends it
to the datalake.

**The datalake S3 key is not an admin key.** `get_bucket_versioning` returns
`AccessDenied`. To turn versioning on, use the MinIO root credentials one time.

**The disk is [rate withheld] full.** [volume withheld] are free. The lake is [volume withheld]. Backups are cheap, but
give them a retention limit.

**Dagster cannot make a job from a user interface.** A job is code in the image. This
is why the scripts are in `ai-platform`.

---

## 4. Phase 1: the catalogue API

Do this first. Everything else needs it.

Make one endpoint. It returns each dataset with its columns, types, descriptions, row
count, object count and size.

The data comes from two places, and they are joined here for the first time:

* **Parquet footers** in MinIO give the column names and the types.
* **The dbt manifest** (`dbt/target/manifest.json`) gives the descriptions.

Cache the result. Refresh it after the nightly publish. Do not read 25 footers for
each keystroke of the autocomplete.

```
new   src/homelab_datalake/api/__init__.py
new   src/homelab_datalake/api/catalogue.py    footers + manifest, joined and cached
new   src/homelab_datalake/api/app.py          Starlette routes, bearer token
new   tests/test_catalogue.py
edit  docker-compose.yml                       add datalake-api, no published port
```

Use `layout.py` for the dataset list. Do not write a path in the new code.

Authenticate with a Keycloak service-account token. Use
`src/homelab_datalake/mcp/auth.py`, which does this already.

---

## 5. Phase 2: write the 384 descriptions

This phase is only content. It needs no code. Do it early, so the tab has data on the
first day.

| Layer | Columns | Have a description | To write |
|---|---|---|---|
| raw | [withheld] | [withheld] | [withheld] |
| curated | [withheld] | [withheld] | [withheld] |
| marts | [withheld] | [withheld] | [withheld] |
| **Total** | **[withheld]** | **[withheld]** | **[withheld]** |

Where to write them:

```
edit  dbt/models/staging/_sources.yml          141 raw columns
edit  dbt/models/staging/_staging.yml          the staging models
new   dbt/models/intermediate/_intermediate.yml
edit  dbt/models/marts/_marts.yml              the remaining 125
```

How to write them correctly:

* For a **raw** column, read the source. The camera API, the Jellyfin API, the NPM log
  format or the LiteLLM table says what the field is.
* For a **curated** or **mart** column, read the SQL that makes it. The expression
  says what the column means.
* If you cannot tell, write `Unclear: <what you do not know>`. Do not invent a
  meaning. A wrong description is worse than no description.

After the user approves the descriptions, add a dbt test. The test must fail if a
column has no description.

---

## 6. Phase 3: the query API and the write switch

All the safety controls go here, not in the TypeScript. The tests for them run with
the other 382 tests in this repository.

### The read-only switch

The switch is on by default.

**Switch on.** DuckDB parses the statement with `con.extract_statements()`. Only
`SELECT` and `EXPLAIN` continue. Do not use a text pattern. A pattern does not see a
comment or unusual spaces. Also set `disabled_filesystems='LocalFileSystem'` and
`lock_configuration=true`, so a query cannot read a file on the server.

**Switch off.** All SQL is permitted, and writes reach the lake. Read section 7
before you build this.

### Limits

| Limit | Value |
|---|---|
| Rows returned | [withheld] |
| Time | 30 seconds |
| Statements in one call | [withheld] |

### The audit log

Write one row for each statement: who, what, when, how long, how many rows, and if
the switch was off. A write that you cannot trace is worse than a write you cannot
make.

```
new   src/homelab_datalake/api/query.py        parse, classify, cap, time out
new   src/homelab_datalake/api/audit.py
new   tests/test_query_guards.py               one test for each refused statement
```

---

## 7. Phase 4: recovery for the lake

**Build this phase before you permit any write.** The user chose to permit writes to
the lake. Raw data is not rebuildable: the NPM logs rotate off the disk, and the
camera serves only a recent window. A deleted raw partition is lost.

Three parts. Build all three.

### Part 1: bucket versioning

Turn on versioning for the `datalake` bucket. MinIO then keeps the previous object
after each overwrite and each delete. Recovery of one object is immediate.

The datalake S3 key cannot do this. Use the MinIO root credentials one time.

Add a lifecycle rule. Delete a non-current version after 30 days. Without the rule,
the bucket grows and does not stop.

```
new   scripts/enable-lake-versioning.sh
```

### Part 2: a daily copy of raw

Versioning does not protect against a person who turns versioning off, or against the
loss of the bucket. Copy `raw/` to a second bucket each day.

Copy only `raw/`. `curated/` and `marts/` are rebuilt from `raw/` by
`analytics_daily`, so a copy of them has no value.

Make it a Dagster asset in the `warehouse` group, so it runs with the nightly job and
you see it in the same graph.

```
new   src/homelab_datalake/assets/backup.py    lake_backup asset
new   tests/test_backup.py
```

### Part 3: a restore that was tested

A backup that nobody restored is not a backup. Write a restore script. Run it. Prove
that it works before the switch is permitted to turn off.

```
new   scripts/restore-lake.sh                  one dataset, one day, or everything
```

The restore script must:

1. Report what it will restore, and stop.
2. Restore only after `--apply`.
3. Compare the object count and the size after the restore.

---

## 8. Phase 5: the Data tab

Copy the shape of **Explore > Tools**. It is the closest thing that exists: a list on
the left, and a detail panel on the right.

```
new   apps/web/src/features/explore/DataView.tsx
new   apps/api/src/routes/datalake.ts           proxy; adds the user identity
edit  apps/web/src/config/navigation.ts         add Data to the Explore group
edit  apps/web/src/app/App.tsx                  add case 'data'
edit  apps/web/src/app/types.ts                 add 'data' to ViewId
edit  packages/contracts/src/index.ts           request and response schemas
edit  apps/web/package.json                     add the CodeMirror packages
```

The layout:

* **Left.** A tree: layer, then domain, then dataset. 25 datasets.
* **Right, top.** The selected table: columns, types, descriptions, partition column,
  rows, objects, size.
* **Right, middle.** The SQL editor, and the read-only switch.
* **Right, bottom.** The results, then the Python cell.
* **Header.** A link to Dagster and a link to the object browser. Both must open the
  selected dataset, not a home page.

The autocomplete uses `@codemirror/lang-sql`. Give it the catalogue from Phase 1. It
then completes a table name after `marts.`, and a column name after
`fct_http_request.`.

Links to build:

* Dagster: `https://data.example.test/assets/<layer>/<table>`
* Objects: `https://console.s3.homelab.example.test/browser/datalake/<prefix>`

---

## 9. Phase 6: the Python sandbox

### Change the image

```
edit  apps/python-sandbox/Dockerfile
```

Change the runtime stage from Alpine to Debian. Install pandas, openpyxl and
python-docx.

Add the module names to the allowlist in
`packages/contracts/src/index.ts`, at `sandboxAllowedModuleSchema`.

### Raise the limits, for the workbench only

Keep the current policy for agent runs. Add a second policy for workbench runs.

| Limit | Agent runs | Workbench runs |
|---|---|---|
| Time | 15 s | 60 s |
| Memory | [volume withheld] | [volume withheld] |
| Input | [volume withheld] | not used, see below |

### Give the sandbox its data

Do **not** put S3 credentials in the sandbox. Do **not** attach the sandbox to another
network. Inject a helper into the Python namespace:

```python
df = datalake.query("SELECT * FROM marts.agg_traffic_daily")   # a DataFrame
datalake.save_workbook(df, "traffic.xlsx")
datalake.save_document(text, "report.docx")
```

`datalake.query` sends the SQL to the API on the internal network. The API sends it to
the datalake query endpoint from Phase 3. The same parser and the same limits apply.
The sandbox still has no route out.

```
edit  apps/python-sandbox/src/runner.ts         inject the helper
edit  apps/api/src/routes/sandbox.ts            the workbench policy, and the relay
```

---

## 10. Phase 7: the script store

A new tab, **Explore > Scripts**.

### What a script is

| Field | Purpose |
|---|---|
| `name` | How a model asks for it |
| `description` | What it does, and when to use it |
| `code` | The Python |
| `parameters` | Name, type, default, for each parameter |
| `version` | Increases on each edit. Old versions stay. |
| `owner` | Who saved it |

### How a script is made

A model writes Python in a chat. The user sees the result. If the user is satisfied,
the user presses **Save as script**. Do not save automatically. The user decides.

### How a model uses a script

Add four MCP tools:

| Tool | What it does |
|---|---|
| `list_scripts` | Names, descriptions and parameters |
| `run_script` | Runs one, with parameters |
| `save_script` | Saves a new one, after the user approves |
| `edit_script` | Makes a new version, after the user approves |

`save_script` and `edit_script` change stored code that other runs will execute. Send
both through the existing approvals system in `apps/api/src/routes/approvals.ts`.

```
new   apps/api/prisma/migrations/<date>_workbench_scripts/migration.sql
new   apps/api/src/routes/scripts.ts
new   apps/web/src/features/explore/ScriptsView.tsx
edit  apps/api/prisma/schema.prisma             model Script, model ScriptVersion
edit  apps/web/src/config/navigation.ts         add Scripts to Explore
edit  packages/agent-runtime/src/mcp-profiles.ts  the four tools
```

---

## 11. Order of work

Phases 1 to 4 are in the datalake. Phases 5 to 7 are in ai-platform.

```
1  catalogue API      ──┬─► 5  Data tab ──► 6  sandbox ──► 7  scripts
2  384 descriptions  ──┘
3  query API + switch ──┘
4  recovery          ─────► then, and only then, permit the switch to turn off
```

Phase 4 must finish before the write switch is released. Phases 1, 2 and 3 can be
released with the switch fixed at read-only.

---

## 12. Risks

| Risk | Why | What to do |
|---|---|---|
| The catalogue is slow | 25 footer reads over the network | Cache; refresh after the nightly publish |
| The autocomplete is old | The schema is read one time | Read it again when the tab gets focus |
| A write breaks the pipeline | The lake feeds `warehouse_tables` | Rebuild the warehouse after a write; show the user this |
| A description is wrong | 384 written in one pass | Write `Unclear:` instead of a guess |
| The backup is never tested | This is usual | Phase 4, part 3 restores before the switch is released |
| The sandbox gets a network | pandas needs data | Use the helper; keep `internal=true` |

---

## 13. What is built, and what is not

Written on 30 August 2026, after the plan above was executed.

### Built and tested

| Phase | Where | State |
|---|---|---|
| 1 catalogue API | `src/homelab_datalake/api/catalogue.py` | Done. 21 tests. |
| 2 the descriptions | `dbt/models/` | Done. 430 written, not 384 — see below. |
| 3 query API and switch | `src/homelab_datalake/api/query.py` | Done. 67 tests. |
| 4 recovery | `assets/backup.py`, two scripts | Code done, 18 tests. **The drill has not been run.** |
| 5 Data tab | `ai-platform` | Done. |
| 6 sandbox | `ai-platform` | Done. Exercised end to end against a stub relay. |
| 7 script store | `ai-platform` | Done. |

`homelab-datalake` is at 496 tests, all passing. `ai-platform` is at 1,017.

### Four places the plan was wrong, and what was done instead

**384 descriptions was 430.** The plan counted YAML column *entries*; many of
those carried only `tests:` and no prose. Every column in every model and source
now has a description, and
[a dbt test](../dbt/tests/assert_every_column_is_documented.sql) fails the build
if one appears without. Descriptions shared by more than one layer are written
once in [`_columns.md`](../dbt/models/_columns.md) and referenced, because a
column that survives from staging into a mart means the same thing in both.

**"Only SELECT and EXPLAIN continue" would have let a write through.** Measured
on DuckDB 1.5: `EXPLAIN ANALYZE DELETE FROM t` classifies as EXPLAIN and
*executes the delete*, and so does `EXPLAIN (ANALYZE) DELETE`. An EXPLAIN is now
permitted exactly when what it explains is permitted, decided by handing the
inner statement back to the same parser.

**`disabled_filesystems='LocalFileSystem'` cannot be set when the lake is local.**
It is the setting that stops `read_csv('/etc/passwd')`, and it also stops reading
a lake that is a directory on disk. It is applied whenever the lake is on S3 —
which is every deployment holding data — and the difference is stated in
`guard_settings` rather than hidden.

**The sandbox runtime could not become `python:3.12-slim`.** That worker is a
Node server; the runtime needs both interpreters, and adding Node to a Python
image is the messier half of the pair. It is `node:22-bookworm-slim` instead,
which is Debian and glibc — the actual reason the plan gave for the change — with
pandas, openpyxl and python-docx in a venv at `/opt/venv`.

### The catalogue refresh is a stat(), not a callback

The plan said "refresh it after the nightly publish". It is done by watching the
serving copy's inode: `warehouse_serving` renames a new one into place as the last
step of the nightly job, so its identity changing *is* the signal. That is one
`stat()` per request instead of a callback Dagster has to make, hold a token for
and retry — there is no refresh that can be lost, because none is sent.
`POST /catalogue/refresh` remains as a manual escape hatch.

### What has to happen before the write switch is released

Phase 4's code is written and unit-tested; none of it has run against the real
bucket. In order:

1. Deploy (`bash scripts/deploy.sh`) so `datalake-api` and `lake_backup` exist.
2. `bash scripts/enable-lake-versioning.sh` — plan first, then `--apply`. Needs
   the MinIO root credentials once; the datalake key cannot do it.
3. Materialise `lake_backup` once, and confirm the object count in the mirror
   matches `raw/`.
4. `bash scripts/restore-lake.sh --dataset npm_access --day <a day>` — plan, then
   `--apply`, and read the verification line. **This is the step the whole phase
   exists for: a backup nobody has restored is not a backup.**
5. Only then set `DATALAKE_API_ALLOW_WRITES=yes` in the datalake's `.env`.

Until step 5 the API refuses every write, whatever the switch in the browser says.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
