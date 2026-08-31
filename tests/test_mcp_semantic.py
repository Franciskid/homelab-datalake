"""The semantic layer: that it is internally consistent, and that it actually binds.

The second half is the one that earns its place. The registry names columns in marts
it does not own, so a rename on the dbt side would leave tools that import fine,
deploy fine, and fail only when somebody asks the question that touches the renamed
column. Binding every expression against a database with the real shape turns that
into a test failure.
"""

from dataclasses import replace

import pytest

from homelab_datalake.mcp.semantic import (
    DATASETS,
    GRAINS,
    get_dataset,
    missing_columns,
    validate,
    validation_statements,
)


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_names_are_unique_within_a_dataset(dataset):
    """A duplicate name would silently shadow, and the second one would never be used."""
    assert len(dataset.dimension_names) == len(set(dataset.dimension_names))
    assert len(dataset.measure_names) == len(set(dataset.measure_names))
    # A dimension and a measure sharing a name would collide in the projection.
    assert not set(dataset.dimension_names) & set(dataset.measure_names)


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_defaults_are_real_measures(dataset):
    assert set(dataset.default_measures) <= set(dataset.measure_names)
    assert dataset.default_measures, "a dataset with no defaults forces every caller to guess"


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_everything_is_documented(dataset):
    """A description is the only thing the model has to choose between two columns."""
    assert dataset.description
    for item in (*dataset.dimensions, *dataset.measures):
        assert item.description, f"{dataset.name}.{item.name} has no description"


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_every_expression_binds_against_the_marts(dataset, con):
    """Every dimension and measure resolves against a database with the real shape."""
    for part, sql in validation_statements(dataset):
        try:
            con.execute(sql).fetchall()
        except Exception as error:  # noqa: BLE001 - re-raised with context below
            pytest.fail(f"{dataset.name}.{part} does not bind: {error}")


def test_validate_reports_a_renamed_column(con, tmp_path):
    """The guard that runs before a serving copy is published."""
    import duckdb

    path = tmp_path / "drifted.duckdb"
    broken = duckdb.connect(str(path))
    broken.execute("CREATE SCHEMA marts;")
    broken.execute(
        "CREATE TABLE marts.fct_llm_call AS SELECT 1 AS call_key, DATE '2026-08-01' AS partition_date"
    )
    problems = validate(broken)
    broken.close()

    assert problems, "a mart missing every column must be reported"
    assert any("llm_spend" in p for p in problems)
    # Every dataset is reported, not just the first, so one deploy fixes all of them.
    assert len({p.split(".")[0] for p in problems}) == len(DATASETS)


def test_unknown_dataset_names_the_alternatives():
    with pytest.raises(KeyError) as raised:
        get_dataset("spend")
    assert "llm_spend" in str(raised.value)


def test_grains_are_a_closed_vocabulary():
    """Grain reaches date_trunc as an identifier, so it may never come from a caller."""
    assert set(GRAINS) == {"day", "week", "month", "quarter", "year"}
    for key, unit in GRAINS.items():
        assert key == unit


# --------------------------------------------------------------------------------
# coverage: the drift `validate` cannot see
# --------------------------------------------------------------------------------
#
# validate() catches a *renamed* column, because the expression stops binding. It
# cannot catch an *added* one: nothing refers to it, everything still binds, and the
# column is invisible to every caller. That is how fct_camera_event reached 78
# columns while the layer went on describing it as a table with no timestamps.


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_every_mart_column_is_either_exposed_or_declared(dataset, con):
    """The check in both directions, against the same shape production has.

    A column added to a mart and not added here is not a small omission: it is a
    caller being told the data does not exist.
    """
    table = dataset.table.rpartition(".")[2]
    columns = [
        row[0] for row in
        con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = ?", [table]
        ).fetchall()
    ]
    assert columns, f"the fixture has no {table}"
    missing = missing_columns(dataset, columns)
    assert not missing, (
        f"{dataset.name} does not mention {missing}. Add each as a dimension, a "
        "measure or an attribute, or name it in not_exposed with the reason."
    )


def test_a_column_nobody_named_is_reported():
    """The check has to fail on the case it exists for, or it is decoration."""
    dataset = get_dataset("camera_activity")
    assert missing_columns(dataset, ["camera_id", "brand_new_column"]) == ["brand_new_column"]


def test_a_column_declared_as_not_exposed_is_not_drift():
    dataset = get_dataset("camera_activity")
    with_reason = replace(
        dataset, not_exposed=(("brand_new_column", "internal bookkeeping"),)
    )
    assert missing_columns(with_reason, ["camera_id", "brand_new_column"]) == []


def test_coverage_reports_a_missing_table_rather_than_passing(con):
    """An empty column list must not read as "nothing is missing"."""
    dataset = replace(get_dataset("camera_activity"), table="marts.fct_nothing_here")
    drifted = coverage_of(con, dataset)
    assert drifted, "a table that is not there has to be reported, not skipped"


def coverage_of(con, dataset):
    from homelab_datalake.mcp import semantic as module

    original = dict(module.DATASETS)
    try:
        module.DATASETS.clear()
        module.DATASETS[dataset.name] = dataset
        return module.coverage(con)
    finally:
        module.DATASETS.clear()
        module.DATASETS.update(original)


# --------------------------------------------------------------------------------
# row-level fields
# --------------------------------------------------------------------------------


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_a_dataset_that_offers_records_can_be_ordered_and_defaulted(dataset):
    if not dataset.attributes:
        return
    assert dataset.record_order in dataset.field_names, (
        f"{dataset.name} orders records by {dataset.record_order!r}, which is not a field"
    )
    assert dataset.default_fields, "a listing with no default columns forces a guess"
    assert set(dataset.default_fields) <= set(dataset.field_names)
    for attribute in dataset.attributes:
        assert attribute.description, f"{dataset.name}.{attribute.name} has no description"


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_every_field_name_resolves_to_exactly_one_expression(dataset):
    """Names are unique among attributes, and an attribute wins over a dimension.

    A name shared with a *measure* is fine and deliberate -- `bytes_sent` is the
    row's value at grain='event' and the sum of them at every other grain, which is
    the same quantity at two resolutions. The two can never appear in one result,
    because measures are refused at grain='event' and fields at every other.
    """
    assert len(dataset.attribute_names) == len(set(dataset.attribute_names))
    assert len(dataset.field_names) == len(set(dataset.field_names))
    for name in dataset.field_names:
        assert dataset.field(name).name == name
    for attribute in dataset.attributes:
        assert dataset.field(attribute.name) is attribute


def test_the_camera_dataset_carries_the_exact_timestamps():
    """The regression this whole change exists for.

    An agent asked for every portal opening with its exact time, read
    describe_dataset, was told the finest resolution was an hour bucket, and
    delivered a different document. These four names are what it needed to see.
    """
    fields = set(get_dataset("camera_activity").field_names)
    assert {"happened_at", "started_at", "occurred_at", "state_observed_at"} <= fields
    assert {"count_person", "count_vehicle", "count_animal"} <= fields
    assert "state_changed" in set(get_dataset("camera_activity").dimension_names)
