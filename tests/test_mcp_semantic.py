"""The semantic layer: that it is internally consistent, and that it actually binds.

The second half is the one that earns its place. The registry names columns in marts
it does not own, so a rename on the dbt side would leave tools that import fine,
deploy fine, and fail only when somebody asks the question that touches the renamed
column. Binding every expression against a database with the real shape turns that
into a test failure.
"""

from dataclasses import replace

import duckdb
import pytest

from homelab_datalake.mcp.semantic import (
    DATASETS,
    GRAINS,
    Dimension,
    create_dataset_views,
    dataset_view_create_statement,
    dataset_view_name,
    describe_entry,
    field_collisions,
    get_dataset,
    measure_entry,
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
# describe_dataset must never show a name a SQL writer cannot use
# --------------------------------------------------------------------------------
#
# Measured against production on 2026-09-16: 10 of 128 dimensions across the two
# Jellyfin datasets carried a `name` that is not a column of the mart (`person` is
# `user_name`, `series` is `series_name`, and so on), and describe_dataset showed
# only the name. An agent asked to chart Jellyfin watch time called describe_dataset,
# wrote `SELECT person FROM marts.fct_jellyfin_playback`, hit `Binder Error:
# Referenced column "person" not found` twice, and told the owner it could not send
# the mail it had been asked for.
#
# describe_dataset's fix is to show the `sql` expression whenever it differs from
# `name` (see `describe_entry`/`measure_entry`), and to show it always for measures.
# The guard below is what stops that contract from silently going false again: for
# every dimension or attribute shown *bare* (no `sql` key, because `sql == name`),
# `name` has to actually be a column of the mart the dataset points at. It uses the
# same `con` fixture -- a DuckDB shaped like the real marts -- that `test_every_
# mart_column_is_either_exposed_or_declared` above already relies on for the mirror
# check (a mart column with no dataset entry at all). This is its complement: a
# dataset entry whose bare name has no mart column behind it.


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_a_bare_name_is_always_a_real_column(dataset, con):
    """A dimension or attribute shown without a `sql` key must bind as its own name.

    `sql == name` is describe_dataset's decision to omit the `sql` key -- it is a
    promise that writing `name` into SQL just works. This checks the promise against
    the same fixture shape production is validated against, so a dataset that renames
    a mart column without giving the affected entry an explicit `sql` fails here
    instead of at the next Binder Error.
    """
    table = dataset.table.rpartition(".")[2]
    columns = {
        row[0].lower() for row in con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name = ?", [table]
        ).fetchall()
    }
    assert columns, f"the fixture has no {table}"
    for item in (*dataset.dimensions, *dataset.attributes):
        if item.sql != item.name:
            continue  # shown with an explicit `sql` key -- not a bare-name claim
        assert item.name.lower() in columns, (
            f"{dataset.name}.{item.name} is shown to a SQL writer as a bare column "
            f"name (its sql == its name), but {item.name!r} is not a column of "
            f"{dataset.table}. Give it an explicit `sql` expression naming the real "
            "column, or, if the mart genuinely has a column by this name, the "
            "fixture in tests/conftest.py is out of date."
        )


def test_describe_entry_omits_sql_only_when_it_says_nothing_new():
    dataset = get_dataset("media_playback")
    person = describe_entry(dataset.dimension("person"))
    assert person == {
        "name": "person", "sql": "user_name",
        "description": dataset.dimension("person").description,
    }
    title = describe_entry(dataset.dimension("title"))
    assert "sql" not in title
    assert title == {"name": "title", "description": dataset.dimension("title").description}


def test_measure_entry_always_carries_sql():
    """A measure is never a bare column, so there is no bare case to omit `sql` in."""
    dataset = get_dataset("media_playback")
    watch_hours = measure_entry(dataset.measure("watch_hours"))
    assert watch_hours["sql"] == "round((sum(play_duration_seconds) / 3600.0)::numeric, 2)"
    assert watch_hours["name"] == "watch_hours"
    for dataset in DATASETS.values():
        for measure in dataset.measures:
            assert "sql" in measure_entry(measure)


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


# --------------------------------------------------------------------------------
# datasets.<name>: the SQL vocabulary view
# --------------------------------------------------------------------------------
#
# The view is what lets a caller write `SELECT person FROM datasets.media_playback`
# and have it simply work, rather than needing to know `person` is `user_name` on
# the mart. These bind it against a database shaped like the real marts -- built
# fresh per test, writably, rather than through the session-scoped `con` fixture:
# the views have to be *created*, and `con` is opened read-only on purpose (see its
# docstring in conftest.py), the same posture the real serving file takes.


@pytest.fixture
def writable_marts(tmp_path):
    """A database shaped like the real marts, open for writing.

    `build_marts` is the same fixture-builder `marts_db`/`con` use, just not opened
    read-only here -- `create_dataset_views` has to `CREATE VIEW`, which a read-only
    handle refuses.
    """
    from tests.conftest import build_marts

    path = str(tmp_path / "warehouse.duckdb")
    build_marts(path)
    connection = duckdb.connect(path)
    connection.execute("SET TimeZone='UTC';")
    yield connection
    connection.close()


def test_no_dataset_has_a_field_collision():
    """A dimension and an attribute of the same name must agree on their SQL, or
    the view has no way to know which one a caller meant."""
    for dataset in DATASETS.values():
        assert field_collisions(dataset) == [], dataset.name


def test_field_collisions_catches_a_real_one():
    """The check has to fail on the case it exists for, or it is decoration."""
    dataset = get_dataset("llm_spend")
    # `call_key` is already an attribute; adding a same-named dimension with a
    # different `sql` is exactly the ambiguity the view cannot resolve by itself.
    colliding = replace(
        dataset,
        dimensions=(*dataset.dimensions, Dimension("call_key", "not_call_key", "collides")),
    )
    assert field_collisions(colliding) == [("call_key", "not_call_key", "call_key")]


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_every_dataset_view_binds_and_exposes_exactly_the_declared_names(dataset, writable_marts):
    """`select * from datasets.<name> limit 0` must return exactly `field_names`,
    in the same order -- the promise `describe_dataset` makes about the view.
    """
    problems = create_dataset_views(writable_marts, datasets=[dataset])
    assert problems == {}, problems
    # S608: `dataset_view_name` is a literal built from the registry, not caller input.
    columns = [
        row[0] for row in
        writable_marts.execute(
            f"SELECT * FROM {dataset_view_name(dataset)} LIMIT 0"  # noqa: S608
        ).description
    ]
    assert columns == dataset.field_names


def test_create_dataset_views_reports_a_collision_rather_than_guessing(writable_marts):
    dataset = get_dataset("llm_spend")
    colliding = replace(
        dataset,
        dimensions=(*dataset.dimensions, Dimension("call_key", "not_call_key", "collides")),
    )
    problems = create_dataset_views(writable_marts, datasets=[colliding])
    assert "llm_spend" in problems
    assert "collision" in problems["llm_spend"]
    # No view was created for it -- a guess would be worse than nothing.
    with pytest.raises(duckdb.Error):
        writable_marts.execute(
            f"SELECT * FROM {dataset_view_name(colliding)} LIMIT 0"  # noqa: S608
        )


def test_create_dataset_views_skips_a_missing_mart_without_failing_the_rest(writable_marts):
    """A dataset added to the registry ahead of its first dbt build must not block
    every other dataset's view from being created."""
    missing = replace(get_dataset("llm_spend"), name="not_built_yet", table="marts.fct_nothing_here")
    problems = create_dataset_views(writable_marts, datasets=[missing, get_dataset("web_traffic")])
    assert "not_built_yet" in problems
    assert "web_traffic" not in problems
    row = writable_marts.execute(
        f"SELECT count(*) FROM {dataset_view_name(get_dataset('web_traffic'))}"  # noqa: S608
    ).fetchone()
    assert row[0] > 0


def test_dataset_view_create_statement_needs_a_source_catalog_when_the_mart_is_elsewhere():
    """`serving.connect` creates the view in its own in-memory catalogue while the
    mart lives in a separately-attached, read-only one -- so the FROM clause has to
    name that catalogue explicitly, or the statement cannot bind."""
    dataset = get_dataset("llm_spend")
    bare = dataset_view_create_statement(dataset)
    qualified = dataset_view_create_statement(dataset, source_catalog="serving")
    assert "FROM marts.fct_llm_call" in bare
    assert "FROM serving.marts.fct_llm_call" in qualified


@pytest.mark.parametrize("dataset", DATASETS.values(), ids=lambda d: d.name)
def test_the_date_column_is_a_date_not_a_timestamp(dataset, con):
    """A range is applied as `date_column BETWEEN start AND end` with whole days.

    On a timestamp the end day is read as its midnight, so everything during the last
    day is silently dropped -- edge_clients first shipped filtering on last_seen_at
    and undercounted by every address seen that day (1,965 instead of 2,004).
    """
    kind = con.execute(f"SELECT typeof({dataset.date_column}) FROM {dataset.table} LIMIT 1").fetchone()
    assert kind and kind[0] == "DATE", f"{dataset.name}.date_column is {kind}"
