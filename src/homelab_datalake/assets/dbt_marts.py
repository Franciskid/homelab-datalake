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

from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
# Only in local development: in the image the manifest is already there, built by
# `dbt parse`, and regenerating it at runtime would be both slower and less
# reproducible than the artefact that was tested.
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_marts(context: AssetExecutionContext, dbt: DbtCliResource):
    """Build and test the models.

    `build`, not `run`: it interleaves each model with its own tests, so a model that
    fails its test does not become a dependency of everything downstream. The privacy
    tests are in that set, which is the point -- a build that would put a raw address
    in a mart fails instead of publishing it.
    """
    yield from dbt.cli(["build"], context=context).stream()
