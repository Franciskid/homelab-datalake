"""The serving copy, and the two properties it exists for.

A read-only DuckDB open *fails* while another process holds the write lock -- not
blocks, fails -- so a reader pointed at the warehouse would error for the length of
every nightly build. The copy removes the contention entirely. Separately, it
contains only the marts, which makes "the serving layer cannot see raw data" a fact
about the file rather than a promise about the code.
"""

import duckdb
import pytest

from homelab_datalake import serving
from homelab_datalake.mcp.warehouse import Warehouse
from tests.conftest import build_marts


@pytest.fixture
def warehouse_file(tmp_path):
    """A warehouse with marts *and* a raw schema, so exclusion is actually tested."""
    path = str(tmp_path / "datalake.duckdb")
    build_marts(path)
    con = duckdb.connect(path)
    con.execute("CREATE SCHEMA raw;")
    con.execute("CREATE TABLE raw.npm_access AS SELECT '203.0.113.7' AS client_address")
    con.execute("CREATE SCHEMA main_staging;")
    con.execute("CREATE TABLE main_staging.stg_x AS SELECT 1 AS a")
    con.close()
    return path


def test_publishing_copies_only_the_marts(warehouse_file, tmp_path):
    target = str(tmp_path / "serving.duckdb")
    published = serving.publish(warehouse_file, target)

    assert published.rows > 0
    con = duckdb.connect(target, read_only=True)
    schemas = {s for (s,) in con.execute(
        "SELECT DISTINCT schema_name FROM duckdb_tables()").fetchall()}
    con.close()
    # Not "raw is empty" -- raw is not in the file at all.
    assert schemas == {"marts"}


def test_the_serving_copy_is_read_only(warehouse_file, tmp_path):
    target = str(tmp_path / "serving.duckdb")
    serving.publish(warehouse_file, target)
    con = serving.connect(target)
    with pytest.raises(duckdb.Error):
        con.execute("CREATE TABLE marts.sneaky AS SELECT 1")
    con.close()


def test_a_drifted_mart_blocks_the_swap(warehouse_file, tmp_path):
    """A dbt rename must fail the deploy, not the first question that touches it."""
    target = tmp_path / "serving.duckdb"
    serving.publish(warehouse_file, str(target))
    good_size = target.stat().st_size

    con = duckdb.connect(warehouse_file)
    con.execute("ALTER TABLE marts.fct_llm_call RENAME COLUMN spend_usd TO cost_usd")
    con.close()

    with pytest.raises(RuntimeError, match="semantic layer"):
        serving.publish(warehouse_file, str(target))

    # The previous good copy is still there and still being served.
    assert target.stat().st_size == good_size
    assert not (tmp_path / "serving.duckdb.building").exists()


def test_publishing_nothing_is_an_error(tmp_path):
    """An empty serving copy answers every question with zero rows and looks fine."""
    empty = str(tmp_path / "datalake.duckdb")
    duckdb.connect(empty).close()
    with pytest.raises(RuntimeError, match="no tables found"):
        serving.publish(empty, str(tmp_path / "serving.duckdb"))


def test_connect_says_what_to_do_when_nothing_is_published(tmp_path):
    with pytest.raises(FileNotFoundError, match="warehouse_serving"):
        serving.connect(str(tmp_path / "absent.duckdb"))


def test_a_reader_survives_the_nightly_swap(warehouse_file, tmp_path):
    """The subtle one: a rename does not disturb an open handle.

    That is what makes the swap safe for a query already running, and it is also how
    a long-lived reader would go on serving last week's numbers forever without ever
    raising. So the connection is reopened when the file underneath it changes.
    """
    target = tmp_path / "serving.duckdb"
    serving.publish(warehouse_file, str(target))

    warehouse = Warehouse(str(target))
    with warehouse.cursor() as cursor:
        before = cursor.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0]

    # A new build lands with a different row count, published under the same name.
    con = duckdb.connect(warehouse_file)
    con.execute("DELETE FROM marts.fct_llm_call WHERE call_key LIKE 'c1%'")
    con.close()
    serving.publish(warehouse_file, str(target))

    with warehouse.cursor() as cursor:
        after = cursor.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0]
    warehouse.close()

    assert after < before, "the reader is still serving the replaced file"


def test_a_plain_connect_would_have_served_stale_data(warehouse_file, tmp_path):
    """Why the serving copy is opened by ATTACH and not by duckdb.connect(path).

    DuckDB caches database instances by path within a process. While any handle is
    open, a later connect() to the same path joins the cached instance instead of
    reading the file -- so after the nightly swap it keeps returning the previous
    build's numbers, silently, for as long as that handle lives.

    This test pins both halves: the naive open is stale, ours is not. If someone
    "simplifies" connect() back to duckdb.connect(path), this fails.
    """
    target = tmp_path / "serving.duckdb"
    serving.publish(warehouse_file, str(target))

    # A long-lived reader opened the way it would be without this design.
    holder = duckdb.connect(str(target), read_only=True)
    before = holder.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0]

    con = duckdb.connect(warehouse_file)
    con.execute("DELETE FROM marts.fct_llm_call WHERE call_key LIKE 'c1%'")
    con.close()
    serving.publish(warehouse_file, str(target))

    naive = duckdb.connect(str(target), read_only=True)
    stale = naive.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0]
    naive.close()

    ours = serving.connect(str(target))
    current = ours.execute("SELECT count(*) FROM marts.fct_llm_call").fetchone()[0]
    ours.close()
    holder.close()

    assert stale == before, "expected the instance cache to hand back the old database"
    assert current < before, "ATTACH must read the file that is on disk now"
