"""The SQL console: what it lets through, and what it does not.

The console is behind Keycloak, so these tests are not about an anonymous attacker.
They are about the cost of a mistake: a query that writes, that reads the filesystem,
or that never finishes, made by somebody who is allowed to be there.
"""

import duckdb
import pytest
from starlette.testclient import TestClient

from homelab_datalake.resources import LakeStorage
from homelab_datalake.sql import app as console
from homelab_datalake.sql import engine


@pytest.fixture
def lake(tmp_path):
    """A lake with one landed dataset, so the layer views have something to bind to."""
    root = tmp_path / "lake"
    target = root / "curated/ai/litellm/spend/partition_date=2026-08-01"
    target.mkdir(parents=True)
    duckdb.connect(":memory:").execute(
        f"COPY (SELECT 1 AS request_id, 0.5 AS spend_usd) "
        f"TO '{target / 'data.parquet'}' (FORMAT PARQUET)"
    )
    return LakeStorage(local_root=str(root))


@pytest.fixture
def sql(marts_db, lake):
    connection = engine.connect(serving_path=marts_db, lake=lake)
    yield connection
    connection.close()


def test_marts_are_queryable(sql):
    rows = engine.run(sql, "SELECT count(*) FROM marts.fct_llm_call").rows
    assert rows == [[200]]


def test_curated_reads_the_lake(sql):
    result = engine.run(sql, "SELECT spend_usd FROM curated.stg_litellm_spend")
    assert result.rows == [[0.5]]


def test_a_dataset_with_nothing_landed_is_absent_not_broken(sql):
    """A source that has never run shows fewer tables, not a page of errors."""
    tables = {t["table"] for t in engine.catalogue(sql).get("curated", [])}
    assert tables == {"stg_litellm_spend"}


def test_catalogue_lists_the_layers_that_have_data(sql):
    layers = engine.catalogue(sql)
    assert "marts" in layers
    assert "fct_http_request" in {t["table"] for t in layers["marts"]}


@pytest.mark.parametrize("statement", [
    "CREATE TABLE marts.x AS SELECT 1",
    "DELETE FROM marts.fct_llm_call",
    "UPDATE marts.fct_llm_call SET model = 'x'",
    "DROP TABLE marts.fct_llm_call",
    "ATTACH '/tmp/other.duckdb' AS other",
    "INSTALL spatial",
    "SET memory_limit = '100GB'",
    "COPY (SELECT 1) TO '/tmp/escape.csv'",
])
def test_only_reads_are_allowed(sql, statement):
    with pytest.raises(engine.QueryRefused):
        engine.run(sql, statement)


def test_the_refusal_names_the_statement_kind(sql):
    with pytest.raises(engine.QueryRefused, match="COPY"):
        engine.run(sql, "COPY (SELECT 1) TO '/tmp/escape.csv'")


def test_several_statements_at_once_are_refused(sql):
    """Otherwise the cap and the timeout apply to whichever one ran last."""
    with pytest.raises(engine.QueryRefused, match="2 statements"):
        engine.run(sql, "SELECT 1; SELECT 2")


def test_the_filesystem_is_out_of_reach(sql):
    with pytest.raises(engine.QueryRefused):
        engine.run(sql, "SELECT * FROM read_csv('/etc/passwd')")


def test_configuration_cannot_be_unlocked(sql):
    with pytest.raises(engine.QueryRefused):
        engine.run(sql, "SET disabled_filesystems=''")


def test_results_are_capped_and_say_so(sql):
    result = engine.run(sql, "SELECT * FROM marts.fct_llm_call", max_rows=10)
    assert len(result.rows) == 10
    assert result.truncated


def test_a_result_under_the_cap_is_not_marked_truncated(sql):
    result = engine.run(sql, "SELECT 1", max_rows=10)
    assert result.rows == [[1]] and not result.truncated


def test_a_runaway_query_is_stopped(sql):
    with pytest.raises(engine.QueryRefused, match=r"stopped|running"):
        engine.run(sql, "SELECT count(*) FROM range(100000000000)", timeout_seconds=1)


def test_explain_is_allowed(sql):
    assert engine.run(sql, "EXPLAIN SELECT 1").rows


def test_a_syntax_error_comes_back_as_a_refusal_not_a_crash(sql):
    with pytest.raises(engine.QueryRefused):
        engine.run(sql, "SELEKT 1")


def test_empty_input_is_refused(sql):
    with pytest.raises(engine.QueryRefused, match="nothing to run"):
        engine.run(sql, "   ")


# -- the HTTP surface ------------------------------------------------------------

@pytest.fixture
def client(monkeypatch, marts_db, lake):
    # Captured before patching: the lambda replaces the name it would otherwise call.
    real = engine.connect
    monkeypatch.setattr(engine, "connect", lambda: real(marts_db, lake))
    return TestClient(console.app)


AUTHED = {"x-forwarded-user": "operator"}


def test_the_page_needs_an_authenticated_user(client):
    """oauth2-proxy sets the header. Without it the app fails closed."""
    assert client.get("/sql/").status_code == 401
    assert client.get("/sql/", headers=AUTHED).status_code == 200


@pytest.mark.parametrize("path", ["/sql/api/schema", "/sql/api/query", "/sql/api/download"])
def test_every_api_route_needs_an_authenticated_user(client, path):
    method = client.get if path.endswith("schema") else client.post
    assert method(path).status_code == 401


def test_query_returns_rows(client):
    response = client.post("/sql/api/query", headers=AUTHED,
                           json={"sql": "SELECT model FROM marts.fct_llm_call LIMIT 2"})
    assert response.status_code == 200
    assert response.json()["columns"] == ["model"]
    assert len(response.json()["rows"]) == 2


def test_a_refused_query_is_a_400_with_the_reason(client):
    response = client.post("/sql/api/query", headers=AUTHED,
                           json={"sql": "DROP TABLE marts.fct_llm_call"})
    assert response.status_code == 400
    assert "read-only" in response.json()["error"]


def test_dates_and_decimals_survive_the_json_round_trip(client):
    """A spend rendered as a float is a different number from the one stored."""
    response = client.post("/sql/api/query", headers=AUTHED, json={
        "sql": "SELECT DATE '2026-08-01' AS d, 0.000000123::DECIMAL(18,9) AS spend"
    })
    assert response.json()["rows"] == [["2026-08-01", "0.000000123"]]


def test_csv_download_carries_the_header_row(client):
    response = client.post("/sql/api/download", headers=AUTHED,
                           data={"sql": "SELECT 1 AS a, 2 AS b"})
    assert response.status_code == 200
    assert response.text.splitlines()[0] == "a,b"
    assert "attachment" in response.headers["content-disposition"]


def test_healthz_does_not_need_a_user(client):
    assert client.get("/healthz").status_code == 200


# -- the production branch of the seal --------------------------------------------
#
# Production reads the lake over httpfs, so it takes the other branch of _seal and
# the fixtures above never touch it. Exercised directly rather than left untested:
# it is the branch that runs on the deployed console.

def test_the_s3_branch_disables_the_local_filesystem(marts_db):
    con = duckdb.connect(":memory:")
    engine._seal(con, LakeStorage(endpoint="https://s3.example.org"), marts_db)
    with pytest.raises(duckdb.Error):
        con.execute("SELECT * FROM read_csv('/etc/hosts')")
    with pytest.raises(duckdb.Error):
        con.execute("SET disabled_filesystems=''")


def test_the_bare_path_redirects_so_relative_fetches_resolve(client):
    response = client.get("/sql", headers=AUTHED, follow_redirects=False)
    assert response.status_code == 308
    assert response.headers["location"] == "/sql/"


@pytest.mark.parametrize("header", ["x-forwarded-user", "x-auth-request-user"])
def test_either_identity_header_counts_as_a_session(client, header):
    """The two come from two different oauth2-proxy settings; either one is proof."""
    assert client.get("/sql/", headers={header: "operator"}).status_code == 200


def test_an_empty_identity_header_is_not_a_session(client):
    assert client.get("/sql/", headers={"x-forwarded-user": ""}).status_code == 401
