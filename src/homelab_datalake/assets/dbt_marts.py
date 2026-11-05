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
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from homelab_datalake import rebuild_state

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
# Only in local development: in the image the manifest is already there, built by
# `dbt parse`, and regenerating it at runtime would be both slower and less
# reproducible than the artefact that was tested.
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
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
        yield from dbt.cli(["build"], context=context).stream()
        return

    plan = rebuild_state.pending()
    if plan.full:
        context.log.info("full build")
        yield from dbt.cli(["build"], context=context).stream()
        rebuild_state.record_built(marts_changed=True)
        return

    build = rebuild_state.plan_build(dbt_project.manifest_path, plan.sources)

    # The steps after this one wait on these outputs. A model left alone still holds
    # correct rows, so it must not read as a skipped step and take the publish with it.
    # They go first and in dependency order because Dagster refuses a model yielded before
    # its upstream, and nothing left alone is downstream of anything being rebuilt.
    outputs = {key: name for name, key in definition.keys_by_output_name.items()}
    graph = {key: definition.asset_deps[key] & outputs.keys() for key in outputs}
    for key in TopologicalSorter(graph).static_order():
        if key.path[-1] in build.untouched_models:
            yield Output(None, output_name=outputs[key], metadata={"rebuilt": False})

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
        yield from dbt.cli(args, context=context).stream()
    else:
        context.log.info("no source dbt reads has changed; nothing to build")

    rebuild_state.record_built(marts_changed=build.marts_changed)
