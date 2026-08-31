# Why the analyst answered the wrong question

On 31 August 2026 a capable model (`gpt-5.6-luna-high`) was asked, three times in
one conversation, for every opening and closing of the entrance gate that month with
the exact time of each. It produced a workbook of daily hour-buckets, then a workbook
of every event from both cameras, and never the thing that was asked for.

The model was not the problem. Every conclusion it drew was correct given what the
tools told it. This is the account of what they told it, because each defect is a
class of defect rather than a one-off, and the fixes are all "make the tool unable to
mislead" rather than "write a better prompt".

[Private conversation and run identifiers omitted.]

---

## 1. The semantic layer had no shape for the question

`run_query` always emitted a `GROUP BY`. Grain went day → week → month → quarter →
year → total, and the finest thing the layer could say about time was a bucket. There
was no way to ask for one row per event, so no amount of reasoning could reach the
answer. The model tried for two full runs and then fell back to something else.

**Fixed** by `grain='event'`, which returns the records themselves with their own
timestamps. It is a grain rather than a separate tool deliberately: `grains` is the
one list a caller reliably reads, and a model shown *day, week, month, quarter,
total* concludes that the day is the floor. Everything that made the aggregate path
safe still holds — identifiers from the registry, bound literals, one table, no join.

## 2. `describe_dataset` said the timestamps did not exist

`fct_camera_event` grew from 29 columns to 78 on 30 August, including five
timestamps. The semantic dataset was not updated, so `describe_dataset` went on
reporting a table whose finest resolution was `started_hour_of_day`.

The model read it and wrote, in run 1 step 20:

> The available camera schema does not expose exact event timestamps—only the event's
> UTC date/hour bucket and state

It repeated that conclusion in all three runs. It was told the data did not exist, so
it stopped looking. **A caller that believes the data is absent does not retry — it
answers a different question.** That is what makes a stale description worse than a
broken one.

`validate()` could not catch this. It binds every expression the layer declares, and
an *undeclared* column binds nothing: every expression still resolved, the deploy was
clean, and 49 columns were invisible.

**Fixed** by checking in the other direction. Every column of a mart must be exposed
as a dimension, a measure or an attribute, or named in `not_exposed` with its reason.
Enforced twice: `tests/test_mcp_semantic.py` against fixtures with the real shape,
and the `semantic_layer_covers_the_marts` check on `warehouse_serving` against the
live copy. All four datasets now cover their marts completely.

This is the same invariant as the `_unmapped` capture at ingestion, one layer up, and
for the same reason: **an omission nobody is told about is indistinguishable from a
decision.**

## 3. The agent's own prompt hardcoded a stale, wrong schema

> The camera table is `main_marts.fct_camera_event` with columns partition_date,
> camera_id, zone_name, rule_type, lifecycle, is_confirmed, should_notify,
> is_unusual, confidence, duration_seconds.

Two failures in one sentence.

`main_marts` is dbt's *build* schema. The serving copy that every reader queries
renames it to `marts`. The one query in the whole conversation that would have
returned the right rows failed with `Catalog Error: schema "main_marts" does not
exist` (run 2, step 21), and the model worked around it by abandoning the query.

And ten columns of seventy-eight, none of them a timestamp — so the escape hatch was
documented as having exactly the same limitation as the tool it was meant to escape.

`describe_schema` reported `main_marts` too. **Fixed**: both names travel and the one
that binds is called `query_as`; the prompt no longer names a table or a column and
says to call `describe_schema` instead, because the warehouse changes and prompt text
does not.

## 4. `describe_schema` was not deployed, and had never been

It was written, registered in `mcp-profiles.ts`, and never built into the running
image — `grep -c describe_schema` in the API container returned 0. So the join graph
was undiscoverable, and it was not in the agent's `toolIds` either. Two independent
reasons it could not be called.

It also advertised "the exact ON clause" and shipped four bare column names, because
`asdict` does not serialise a property. And it carried no columns at all, so the only
way to discover one was `describe_dataset` — the tool that had gone stale.

## 5. The saved workflow it fell back to lied about its parameters

`export-raw-camera-events` v10 declares `START`, `END` and `CAMERAS`, and then:

```python
START = "2026-08-07"          # overwrites the argument
END = "2026-08-30"            # overwrites the argument
CAMERAS_SELECTED = ["Entrance"]
...
AND camera_id IN ('indoor-camera', 'outdoor-camera')   # both cameras, regardless
```

This is the workbook that "contains EVERYTHING". The model called it with
`START=2026-08-01, END=2026-08-31, CAMERAS=["Entrance"]` and reported those values
back to the user, because that is what it asked for. The script had thrown them away.

Both saved workflows are archived — the other aggregated to one row per day, the
shape that was rejected. Neither is needed: the dataset tools now express both
directly. An edit un-archives them.

## 6. Smaller things that each cost a step

- `run_script` rejected `CAMERAS: ["Entrance"]` with `arguments.CAMERAS: Invalid
  input` — it wanted the JSON *string*. No hint in the message. Cost two steps
  across two runs.
- `maxToolSteps` was 12 and ran out mid-task (run 2, step 37). Now 30.
- The sandbox's `datalake.query` dropped the relay's `truncated` flag, so a script
  could not tell a partial answer from a whole one. It now raises. That fix existed
  in the working tree for a day and had never built: a backtick in a Python docstring
  closed the TypeScript template literal it lives inside.

---

## What to take from this

Three of the six defects are the same defect: **a second place that describes the
data, maintained by hand, drifting behind the first.** The semantic cube, the agent's
prompt, and the test fixtures each restated a schema they did not own.

The registries that now cannot drift silently are the ones with a check that runs the
comparison in the direction that catches an *addition*, not just a rename:

| Registry | Drifts behind | Caught by |
|---|---|---|
| `mcp/semantic.py` datasets | the marts | `semantic_layer_covers_the_marts`, `test_every_mart_column_is_either_exposed_or_declared` |
| `assets/warehouse.py::_SOURCES` | `layout.RAW_DATASETS` | `tests/test_warehouse_tables.py` |
| projection allowlists | the source APIs | `_unmapped` + `no_unmapped_fields` |
| `tests/conftest.py` fixtures | the marts | the coverage test above, run against the fixtures |

The agent's system prompt is the one that still cannot be checked, which is why it no
longer contains a schema at all.

> Public history note: private identifiers, measurements, and operational details were redacted. Code examples use placeholder infrastructure.
