"""The dbt project, as Dagster assets.

Until now dbt ran by hand. That was a real gap rather than a stylistic one: the raw
partitions landed on a schedule and the marts did not, so anything reading the
warehouse was reading whatever the last manual `dbt build` produced. A serving layer
over stale marts is worse than no serving layer, because it looks fine.

It is wired in as `@dbt_assets` rather than one asset that shells out to `dbt build`,
and the difference is the whole argument for using Dagster at all. One opaque asset
would give a single green or red dot for twenty models -- which is what cron already
does. Per-model assets mean the lineage runs unbroken from a Parquet partition
through staging and intermediate to a mart, a failing model names itself, and
rebuilding one mart does not rebuild the other nineteen.

Each `source('raw', 'x')` in the dbt project becomes the asset key `["raw", "x"]`,
which is exactly the key the warehouse multi-asset emits. That is what makes the two
graphs one graph: no translator, no mapping table, just the same name on both sides.

The manifest is compiled at image build time (`dbt parse` in the Dockerfile), not on
startup. Parsing on load would make the code location's boot time depend on dbt, and
a parse error would present as a code location that will not load rather than as the
dbt error it is.
"""

from graphlib import TopologicalSorter
from pathlib import Path

from dagster import AssetExecutionContext, Output
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from homelab_datalake import build_report, rebuild_state

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
# Only in local development: in the image the manifest is already there, built by
# `dbt parse`, and regenerating it at runtime would be both slower and less
# reproducible than the artefact that was tested.
dbt_project.prepare_if_dev()


class _NonBlockingTests(DagsterDbtTranslator):
    """dbt tests as asset checks that report but do not block.

    dagster-dbt makes every severity=error test a *blocking* check, and a failed
    blocking check fails the whole step: one bad source took the publish, and so every
    dataset, down with it. Blocking is not what protects readers here. `dbt build`
    already skips everything downstream of a failed test, and the publish serves a
    mart whose own test failed from the previous copy (see build_report).
    """

    def get_asset_check_spec(self, asset_spec, manifest, unique_id, project):
        spec = super().get_asset_check_spec(asset_spec, manifest, unique_id, project)
        if spec is None or not spec.blocking:
            return spec
        from dagster._record import replace

        return replace(spec, blocking=False)


_FAILED = {"error", "fail", "runtime error"}


def _failures(invocation) -> tuple[list[dict[str, str]], list[str]]:
    """What this dbt invocation failed, and which models must not be published as built.

    A model whose own test failed has already been rebuilt with the rows that failed
    it, so that table is held back. A model that errored kept its previous table,
    and one skipped because something upstream failed was never touched: both are
    safe to serve as they are.
    """
    try:
        results = invocation.get_artifact("run_results.json").get("results", [])
    except Exception as error:  # noqa: BLE001 - no artifact at all: dbt never got going
        return [{"stage": "dbt", "name": "dbt build", "detail": f"no run_results.json: {error}"}], []
    manifest = invocation.manifest
    nodes = manifest.get("nodes", {})
    failures: list[dict[str, str]] = []
    held: set[str] = set()
    for result in results:
        status = str(result.get("status", "")).lower()
        if status not in _FAILED:
            continue
        unique_id = result.get("unique_id", "")
        node = nodes.get(unique_id, {})
        failures.append({
            "stage": "dbt",
            "name": node.get("name") or unique_id,
            "detail": f"{status}: {str(result.get('message') or '')[:300]}",
        })
        if unique_id.startswith("test."):
            for parent in manifest.get("parent_map", {}).get(unique_id, []):
                if parent.startswith("model."):
                    model = nodes.get(parent, {})
                    held.add(model.get("alias") or model.get("name") or parent.split(".")[-1])
    if not failures and not invocation.is_successful():
        failures.append({"stage": "dbt", "name": "dbt build", "detail": "dbt exited non-zero with no failed node"})
    return failures, sorted(held)


def _build(context, dbt: DbtCliResource, args: list[str]):
    """Run dbt without letting a failure end the step. Returns (outputs produced, invocation)."""
    invocation = dbt.cli(args, context=context, raise_on_error=False)
    produced: set[str] = set()
    for event in invocation.stream():
        if isinstance(event, Output):
            produced.add(event.output_name)
        yield event
    return produced, invocation


def _settle(context, definition, already: set[str], invocation):
    """Placeholders for every model dbt did not build, then record what failed.

    Without an output, a model that failed or was skipped reads to Dagster as a
    skipped step and takes the publish down with it. Its table is still the last
    good one, so it gets an output saying it was not rebuilt.
    """
    failures, held = _failures(invocation)
    outputs = {key: name for name, key in definition.keys_by_output_name.items()}
    graph = {key: definition.asset_deps[key] & outputs.keys() for key in outputs}
    for key in TopologicalSorter(graph).static_order():
        name = outputs[key]
        if name not in already:
            yield Output(None, output_name=name, metadata={"rebuilt": False})
    if failures:
        build_report.record(context.run_id, failures, held)
        context.log.error(
            "dbt: %d failure(s); everything else was built. Held back: %s. %s",
            len(failures), held or "none", "; ".join(f["name"] for f in failures[:20]),
        )
    return failures


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=_NonBlockingTests())
def dbt_marts(context: AssetExecutionContext, dbt: DbtCliResource):
    """Build and test the models downstream of what changed. See rebuild_state.

    `build`, not `run`: it interleaves each model with its own tests, so a model that
    fails its test does not become a dependency of everything downstream. The privacy
    tests are in that set, which is the point -- a build that would put a raw address
    in a mart fails instead of publishing it.
    """
    definition = context.assets_def
    if definition.keys_by_output_name != definition.node_keys_by_output_name:
        # Someone materialised a few models by hand. They asked for those, and building
        # a few says nothing about the state of the rest.
        produced, invocation = yield from _build(context, dbt, ["build"])
        yield from _settle(context, definition, produced, invocation)
        return

    plan = rebuild_state.pending()
    if plan.full:
        context.log.info("full build")
        produced, invocation = yield from _build(context, dbt, ["build"])
        failures = yield from _settle(context, definition, produced, invocation)
        # Left pending on a failure, so the next run tries the same build again.
        if not failures:
            rebuild_state.record_built(marts_changed=True)
        return

    build = rebuild_state.plan_build(dbt_project.manifest_path, plan.sources)

    # The steps after this one wait on these outputs. A model left alone still holds
    # correct rows, so it must not read as a skipped step and take the publish with it.
    # They go first and in dependency order because Dagster refuses a model yielded before
    # its upstream, and nothing left alone is downstream of anything being rebuilt.
    outputs = {key: name for name, key in definition.keys_by_output_name.items()}
    graph = {key: definition.asset_deps[key] & outputs.keys() for key in outputs}
    produced: set[str] = set()
    for key in TopologicalSorter(graph).static_order():
        if key.path[-1] in build.untouched_models:
            yield Output(None, output_name=outputs[key], metadata={"rebuilt": False})
            produced.add(outputs[key])

    if build.rebuilds_anything:
        context.log.info(
            "partial build, sources changed: %s; leaving alone %d models",
            list(plan.sources), len(build.untouched_models),
        )
        # An exclude list rather than `--select source:raw.x+`, because dagster-dbt adds
        # its own `--select fqn:*`, and dbt builds the union of the two: everything.
        # Indirect selection off because by default excluding a model also drops any test
        # touching it, including a test joining it to a model being rebuilt, which is
        # exactly the test that must run. The list names every test to leave out instead.
        args = ["build", "--indirect-selection", "empty"]
        if build.exclude:
            args += ["--exclude", " ".join(build.exclude)]
        built, invocation = yield from _build(context, dbt, args)
        failures = yield from _settle(context, definition, produced | built, invocation)
        if failures:
            return
    else:
        context.log.info("no source dbt reads has changed; nothing to build")

    rebuild_state.record_built(marts_changed=build.marts_changed)
