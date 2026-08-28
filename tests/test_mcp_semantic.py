"""The semantic layer: that it is internally consistent, and that it actually binds.

The second half is the one that earns its place. The registry names columns in marts
it does not own, so a rename on the dbt side would leave tools that import fine,
deploy fine, and fail only when somebody asks the question that touches the renamed
column. Binding every expression against a database with the real shape turns that
into a test failure.
"""

import pytest

from homelab_datalake.mcp.semantic import (
    DATASETS,
    GRAINS,
    get_dataset,
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
