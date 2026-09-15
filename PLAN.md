# homelab-datalake — plan

A lakehouse over three homelab data sources, with an MCP layer on top so models can query it
and produce formatted deliverables. Notes from a design session on 2026-08-27.

[Private career context or local document reference omitted.]

---

## 1. Why this repo exists

Demonstrate a working data pipeline over self-hosted applications.

---

## 2. Scope

One repo, three heterogeneous sources, one warehouse, one orchestrator, one test suite.

Three separate toy projects would be weaker, because one platform ingesting genuinely
different shapes of data is what a real company data platform looks like.

### Source A — computing / infra logs
Nginx, Cloudflare, host and container metrics. Highest volume, closest analogue to real work.

**IPs are personal data under GDPR: hash or truncate at ingestion, and say so in the README.**
Handling PII at the boundary rather than as an afterthought is exactly what a fintech screens
for. That one design note does real work in an interview.

**Amended 2026-09-14 (WS-41.E2):** the owner decided the warehouse should also keep the raw
client address and the raw query string, alongside the hash/network/geo columns above rather
than instead of them -- see `src/homelab_datalake/privacy.py` for the reasoning and the
guarantees that still hold. This does not touch the design note above: hashing and truncation
still happen at ingestion, unconditionally, and the raw columns are additive and
private-warehouse-only -- never in `seed/`, never in anything `publish.py`/`serving.py` sends
somewhere wider than the raw/curated/marts layers already reach. What changed is the answer to
"does this warehouse throw the address away completely", and the answer is no longer yes.

### Source B — camera events
Technically the most interesting. Semi-structured LLM JSON into structured event tables gives
schema evolution, validation of untrusted model output, confidence handling, dedup.

The hard validation logic already exists in `homelab-camera` (fixed species vocabulary,
rewriting unsupported detections to `nothing`, distinguishing missing confidence from low
confidence). The pipeline just has to land it in a warehouse.

Data stays on the server. See §4.

[Private career context or local document reference omitted.]

Use library **metadata**, not playback history — metadata is safe to publish as a seed,
playback history is personal and adds nothing to the demonstration.

**Together:** volume + semi-structured + external API enrichment. Covers most of what the job
actually involves.

---

## 3. Architecture

```
Dagster assets (daily partitions + one MinIO sensor)
  → Parquet landing in MinIO, partitioned by date
  → DuckDB warehouse
  → dbt: staging / intermediate / marts
  → dbt tests + ingestion-level quality checks
  → Evidence.dev or Grafana
  → GitHub Actions: dbt build against the synthetic seed on every push
  → MCP server (§7) exposing typed tools over the marts
```

### Ingestion design
Daily, built on **partitioned assets** — the primitive that makes this a pipeline rather than
a cron script.

- `DailyPartitionsDefinition` on each source asset. Every day is addressable: re-runnable,
  backfillable, repairable on its own.
- **Nginx / Cloudflare** — already rotate daily. Land yesterday's partition each morning.
  Hash/truncate IPs here, at the boundary.
- **Camera events** — metadata JSON already goes to MinIO. Partition by event date.
- **Jellyfin** — small enough to snapshot fully daily. SCD2 if you want to show off library
  history.
- **Make one source sensor-driven** rather than scheduled — a MinIO sensor firing when new
  clips land. Having both batch schedules and an event sensor in the same project shows you
  know when each applies. You will be asked.

**The property to build for and state in the README: re-running a partition replaces it
exactly.** Idempotent, re-runnable, backfillable. That is the line between real and tutorial.

---

## 4. Two decisions already made

### Don't build an ingestion UI — use Dagster's
Dagster ships a web UI: asset graph, run history, per-partition status, materialise button,
backfill dialog, schedule and sensor management, failure logs. That *is* visual, interactive
ingestion management. Free, and instantly recognisable to anyone hiring.

[Private career context or local document reference omitted.]

Simpler on the orchestration UI. Ambitious on the MCP layer.

### Don't randomise camera event timestamps
The idea was to jitter event times so an attacker couldn't infer recurrences from published
data. Rejected:

1. **It fabricates the analysis.** Temporal patterns *are* the analytics on event data —
   hourly distribution, weekday vs weekend, seasonality. Jitter them and the dashboard's
   central insight is fiction. Worse than publishing nothing: if anyone works it out, the
   credibility of the whole repo goes with it.
2. **It doesn't work.** Jitter small enough to preserve the patterns preserves them for an
   attacker too — daily counts and weekday structure survive most noise. Jitter large enough
   to destroy that also destroys the analysis. There is no window where both hold.
3. **It contradicts the `homelab-camera` argument.** That repo argues against handing over the
   map, and ships an anonymise mode to demo safely. Publishing the house's occupancy data one
   repo over undercuts it. An interviewer who notices reads inconsistency, not thoroughness.

**The unlock: a data portfolio repo does not publish the data.** It publishes the code —
models, tests, orchestration — plus a small synthetic seed for reproducibility, plus
screenshots of the real dashboards. Camera events can absolutely be a source; they just stay
on the server. Nobody expects production data in a portfolio repo. Shipping it would be the
odd choice.

---

## 5. What gets published vs what stays private

| | Public | Private |
|---|---|---|
| Pipeline code, dbt models, tests, CI | ✅ | |
| Synthetic seed dataset | ✅ | |
| Screenshots of real dashboards | ✅ | |
| Dagster UI screenshots | ✅ | |
| Real infra logs, camera events, library data | | ✅ |
| Credentials, endpoints, network topology | | ✅ |

---

## 6. The checklist that matters more than the dataset

Most portfolio data projects are tutorials with a nice dashboard. Ship these and this one
isn't — this is what a data lead is actually scanning for:

- [ ] Incremental, idempotent loads — running twice changes nothing
- [ ] A backfill path, and a documented case where it was used
- [ ] Schema evolution handled explicitly
- [ ] Data quality tests that **fail the pipeline**, not just log
- [ ] A partitioning decision defended with real before/after numbers
- [ ] Orchestration with retries and alerting on failure
- [ ] CI running the tests on every push
- [ ] PII handled at the ingestion boundary, and said so
- [ ] A README explaining *why* each choice, with one real cost or performance figure

---

## 7. The MCP layer (the headline)

MCP server over the warehouse so models on `ai.example` can pull data and produce formatted
deliverables — Excel, Word — from a plain-language query, using the existing code sandbox.

**Why this is the strongest single thing to build here:**
- It's a live product category in 2026 — several Paris startups are building exactly this.
  Having shipped one puts you in the conversation as a builder, not a reader.
- It unifies all three threads: data engineering + self-hosted AI + shipping end-to-end. One
  artifact proving the whole positioning instead of three proving it separately.
- It demos in 60 seconds, and video is the format that already works best here. "Here's a
  question in plain French, here's the Excel that comes out" is a devastating demo.
- Reusing the existing sandbox makes it a composition of platform pieces you already own, not
  a one-off. Better story.

**Design caveats — an interviewer will probe exactly here:**

- **Don't build "model writes arbitrary SQL."** Text-to-SQL is the easy part and the fragile
  part. Build **typed tools with constrained parameters** over a small semantic layer —
  `revenue_by_period(start, end, grain)`, not `run_sql(str)`. Being able to explain *why* you
  chose that is worth more than the feature itself.
- **Read-only, scoped, bounded.** Read-only DuckDB connection, query timeouts, row caps.
  Expect "what stops a model running a cartesian join at 3am?"
- **The Excel/Word generation is the tangible half.** A query becoming a formatted `.xlsx` is
  a deliverable, not a chatbot reply. Lean into it.
- **Handle the empty and the wrong answer.** What the tool returns on zero rows, and how you
  stop the model narrating a confident summary over nothing. That detail separates a demo from
  something usable.

---

## 8. Sequencing

The failure mode is an ambitious repo that's [rate withheld] done when someone asks for the GitHub link.
Ship in stages, each one complete and published.

**Stage 1 (~1 week) — infra logs only.** Daily partitions, dbt models, tests, CI, Dagster UI
screenshot in the README. **Stop and publish.** This alone is a complete portfolio piece and
already closes the coherence gap in §1.

**Stage 2 — add camera events and Jellyfin.** Now it's heterogeneous sources and the
semi-structured validation work shows up.

**Stage 3 — the MCP server.** Demo video. Becomes the headline, and probably what to lead with
on the GitHub profile and in conversations.

[Private career context or local document reference omitted.]

---

## 9. Next step

Scaffold Stage 1: Dagster project with the daily-partitioned log assets, dbt skeleton,
synthetic seed generator, CI workflow, README. Start from something that runs.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
