"""The read-only switch, and everything it has to refuse.

One test per refused shape, because this is the file that says what "read-only"
means. The guarantee is not "we check for the word DELETE" -- it is that the only
statements that run with the switch on are ones DuckDB's own parser calls a SELECT,
and that nothing a caller can put in a statement changes that classification.

The obfuscation cases are not hypothetical paranoia. A block comment between two
keywords, a newline instead of a space, and `EXPLAIN ANALYZE` in front of a write are
each enough to get past a pattern, and the last one *executes* -- measured on DuckDB
1.5, see homelab_datalake.api.query.
"""

import json
import threading

import duckdb
import pyarrow as pa
import pytest
from starlette.testclient import TestClient

from homelab_datalake.api import query as q
from homelab_datalake.api.app import create_app
from homelab_datalake.api.audit import AuditLog, Entry
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def lake(tmp_path) -> LakeStorage:
    storage = LakeStorage(local_root=str(tmp_path / "lake"))
    storage.write_table(
        "marts/infrastructure/agg_traffic_daily/partition_date=2026-08-01/data.parquet",
        pa.table({"requests": pa.array([1, 2, 3]), "host": pa.array(["a", "b", "c"])}),
    )
    return storage


@pytest.fixture
def session(lake):
    with q.LakeSession(lake=lake) as built:
        yield built


@pytest.fixture
def parser():
    """A bare connection, for classification tests that need no data."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def _classify(parser, sql: str) -> bool:
    return q.is_read_only(parser, q.single_statement(parser, sql))


# --------------------------------------------------------------------------------
# what the switch refuses, one shape at a time
# --------------------------------------------------------------------------------


@pytest.mark.parametrize("sql", [
    "INSERT INTO marts.agg_traffic_daily VALUES (1, 'x')",
    "UPDATE marts.agg_traffic_daily SET requests = 0",
    "DELETE FROM marts.agg_traffic_daily",
    "MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE",
    "CREATE TABLE scratch AS SELECT 1",
    "CREATE OR REPLACE VIEW marts.agg_traffic_daily AS SELECT 1",
    "DROP VIEW marts.agg_traffic_daily",
    "ALTER TABLE t RENAME TO u",
    "COPY (SELECT 1) TO 's3://datalake/raw/x/data.parquet'",
    "ATTACH 'other.duckdb' AS other",
    "DETACH other",
    "SET s3_access_key_id='stolen'",
    "PRAGMA disabled_filesystems=''",
    "INSTALL httpfs",
    "LOAD httpfs",
    "CALL pragma_table_info('x')",
    "EXPORT DATABASE '/tmp/dump'",
    "VACUUM",
    "PREPARE p AS SELECT 1",
])
def test_a_statement_that_is_not_a_select_is_refused(parser, sql):
    assert _classify(parser, sql) is False


@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "select * from marts.agg_traffic_daily",
    "WITH a AS (SELECT 1) SELECT * FROM a",
    "FROM range(3)",
    "SELECT 1 UNION ALL SELECT 2",
    "PRAGMA version",
    "EXPLAIN SELECT 1",
    "EXPLAIN ANALYZE SELECT 1",
    "EXPLAIN (ANALYZE) SELECT 1",
    "EXPLAIN (FORMAT json) SELECT 1",
])
def test_a_select_and_an_explain_of_one_are_permitted(parser, sql):
    assert _classify(parser, sql) is True


# --------------------------------------------------------------------------------
# the ways a pattern would be fooled
# --------------------------------------------------------------------------------


def test_a_comment_between_the_keywords_does_not_hide_a_write(parser):
    """The parser sees through this. A regular expression looking for "delete" would
    match here too -- but one looking for "^\\s*select" would let it past."""
    assert _classify(parser, "/* SELECT */ DELETE /* SELECT */ FROM marts.x") is False


def test_unusual_whitespace_does_not_change_the_classification(parser):
    assert _classify(parser, "\n\t  delete\n\tfrom\n\tmarts.x") is False


def test_explain_analyze_of_a_write_is_refused_because_it_executes(parser):
    """EXPLAIN ANALYZE runs the statement. Both spellings, and both are writes."""
    assert _classify(parser, "EXPLAIN ANALYZE DELETE FROM marts.x") is False
    assert _classify(parser, "EXPLAIN (ANALYZE) DELETE FROM marts.x") is False
    assert _classify(parser, "/*a*/EXPLAIN/*b*/ANALYZE/*c*/DELETE FROM marts.x") is False


def test_explain_analyze_really_does_execute():
    """The measurement the rule above exists for, kept as a test so it stays true.

    If a DuckDB upgrade makes EXPLAIN ANALYZE inert, this fails and the rule can be
    relaxed deliberately rather than discovered to have been unnecessary.
    """
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t AS SELECT 1 AS a UNION ALL SELECT 2")
    con.execute("EXPLAIN DELETE FROM t WHERE a = 1")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 2  # planned only
    con.execute("EXPLAIN ANALYZE DELETE FROM t WHERE a = 1")
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 1  # executed
    con.close()


def test_a_second_statement_cannot_ride_along(parser):
    with pytest.raises(q.QueryRefused, match="2 statements"):
        q.single_statement(parser, "SELECT 1; DELETE FROM marts.x")


def test_a_semicolon_inside_a_string_is_not_a_separator(parser):
    """Which is why the statements are counted by the parser, not by splitting."""
    statement = q.single_statement(parser, "SELECT 'a; b' AS s")
    assert q.is_read_only(parser, statement) is True


def test_an_unparseable_statement_is_refused_with_the_parser_s_message(parser):
    with pytest.raises(q.QueryRefused, match="could not be parsed"):
        q.single_statement(parser, "SELEKT 1")


def test_an_empty_statement_is_refused(parser):
    with pytest.raises(q.QueryRefused, match="no statement"):
        q.single_statement(parser, "   \n  ")


# --------------------------------------------------------------------------------
# the engine's own guards
# --------------------------------------------------------------------------------


def test_the_local_filesystem_is_disabled_when_the_lake_is_on_s3():
    """A SELECT is not harmless if it can be SELECT * FROM read_csv('/etc/shadow')."""
    on_s3 = LakeStorage(endpoint="https://s3.example", bucket="datalake")
    assert q.guard_settings(on_s3)["disabled_filesystems"] == "LocalFileSystem"


def test_a_local_lake_cannot_disable_the_filesystem_it_lives_on():
    """Stated rather than hidden: development reads the lake off the disk, so the
    same setting that blocks /etc/passwd would block the data. The deployment that
    holds real data is the one that is locked down."""
    local = LakeStorage(local_root=".lake")
    assert "disabled_filesystems" not in q.guard_settings(local)


def test_the_configuration_is_locked_whatever_the_backend():
    """Without the lock the caller sets disabled_filesystems back to empty."""
    assert q.guard_settings(LakeStorage(local_root=".lake"))["lock_configuration"] == "true"
    assert q.guard_settings(LakeStorage(endpoint="https://s3.example"))["lock_configuration"] == "true"


def test_the_lock_is_applied_last(lake):
    """It blocks every SET after it, including the ones that set up the session."""
    settings = list(q.guard_settings(LakeStorage(endpoint="https://s3.example")))
    assert settings[-1] == "lock_configuration"


def test_a_read_only_session_cannot_be_reconfigured(session):
    cursor = session.cursor()
    with pytest.raises(duckdb.Error, match="Cannot change configuration"):
        cursor.execute("SET memory_limit='16GB'")


def test_the_s3_settings_are_global_so_a_cursor_inherits_them(lake, monkeypatch):
    """The bug this exists for was invisible until the first query against MinIO.

    Every s3_* setting is LOCAL by default, and a cursor inherits none of its
    parent's local settings. So the views bound on the connection -- which had the
    credentials -- and every query, which runs on a cursor, read the bucket
    anonymously and came back with a 403 saying the credentials were missing.

    Nothing on a local backend can catch that: the local lake sets no s3_* settings
    at all. So what is asserted here is the *shape* of the configuration.
    """
    scopes = []
    original = LakeStorage.configure_duckdb

    def spy(self, con, scope=""):
        scopes.append(scope)
        return original(self, con, scope=scope)

    monkeypatch.setattr(LakeStorage, "configure_duckdb", spy)
    with q.LakeSession(lake=lake):
        pass

    assert scopes == ["GLOBAL"]


def test_every_s3_setting_is_applied_at_the_scope_it_was_asked_for():
    """A setting left at session scope is one a cursor will not see."""
    on_s3 = LakeStorage(
        endpoint="https://s3.example", bucket="datalake",
        access_key_id="key", secret_access_key="secret",
    )
    statements = []

    class Recorder:
        def execute(self, sql):
            statements.append(sql)

    on_s3.configure_duckdb(Recorder(), scope="GLOBAL")

    assert statements[0] == "INSTALL httpfs; LOAD httpfs;"
    assert all(sql.startswith("SET GLOBAL ") for sql in statements[1:])
    assert {name for name, _ in on_s3.duckdb_settings()} == {
        "s3_endpoint", "s3_region", "s3_access_key_id",
        "s3_secret_access_key", "s3_url_style", "s3_use_ssl",
    }


def test_a_local_lake_needs_no_settings_at_all():
    """Which is exactly why the bug above could not show up in these tests."""
    assert LakeStorage(local_root=".lake").duckdb_settings() == []
    statements = []

    class Recorder:
        def execute(self, sql):
            statements.append(sql)

    LakeStorage(local_root=".lake").configure_duckdb(Recorder(), scope="GLOBAL")
    assert statements == []


def test_a_quote_in_a_configured_value_cannot_end_the_statement():
    on_s3 = LakeStorage(endpoint="https://s3.example", secret_access_key="a'b")
    statements = []

    class Recorder:
        def execute(self, sql):
            statements.append(sql)

    on_s3.configure_duckdb(Recorder())
    assert "SET s3_secret_access_key='a''b';" in statements


def test_a_read_only_session_is_pinned_to_utc(session):
    """A cursor does not inherit its parent's session settings, and cannot set them
    once the configuration is locked -- so the pin has to be in the connect config.
    Without it a 23:30 UTC row lands on the wrong day on a Europe/Paris host."""
    assert session.cursor().execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"


# --------------------------------------------------------------------------------
# the lake, mounted
# --------------------------------------------------------------------------------


def test_the_lake_is_readable_by_its_qualified_name(session):
    result = q.run(session, "SELECT sum(requests) AS total FROM marts.agg_traffic_daily")
    assert result.rows == [[6]]
    assert result.columns == ["total"]


def test_an_unqualified_name_resolves_through_the_search_path(session):
    assert q.run(session, "SELECT count(*) FROM agg_traffic_daily").rows == [[3]]


def test_a_dataset_with_nothing_landed_is_named_rather_than_breaking_the_session(session):
    """One empty prefix must not make every other table unqueryable."""
    assert "marts.agg_traffic_daily" in session.mounted
    assert "raw.npm_access" in session.missing
    assert q.run(session, "SELECT 1 AS ok").rows == [[1]]


def test_a_missing_table_comes_back_as_the_caller_s_mistake(session):
    with pytest.raises(q.QueryRefused, match="nope"):
        q.run(session, "SELECT * FROM marts.nope")


# --------------------------------------------------------------------------------
# the limits
# --------------------------------------------------------------------------------


def test_rows_are_capped_and_the_truncation_is_declared(session):
    result = q.run(session, "SELECT i FROM range(5000) t(i)", max_rows=10)
    assert result.row_count == 10
    assert result.truncated is True
    assert any("Truncated to 10 rows" in note for note in result.notes)


def test_an_answer_that_fits_is_not_declared_truncated(session):
    result = q.run(session, "SELECT i FROM range(3) t(i)", max_rows=10)
    assert result.row_count == 3
    assert result.truncated is False
    assert result.notes == []


def test_the_cap_does_not_wait_for_the_whole_scan(session):
    """DuckDB streams, so the cap is applied by fetching rather than by rewriting the
    caller's SQL. Twenty million rows in under a second is the evidence."""
    import time

    began = time.perf_counter()
    result = q.run(session, "SELECT i FROM range(20000000) t(i)", max_rows=5)
    assert result.row_count == 5
    assert time.perf_counter() - began < 5.0


def test_a_statement_that_outlives_the_deadline_is_cancelled(session):
    with pytest.raises(q.QueryRefused, match="exceeded the"):
        q.run(session, "SELECT count(*) FROM range(100000000000) t(i)", timeout=0.3)


def test_the_deadline_does_not_leak_a_timer_thread(session):
    """The watchdog is cancelled on the way out, including when the statement fails.

    A leaked watchdog is invisible until it fires: thirty seconds later it interrupts
    whatever that connection is doing, which reads as an unrelated query mysteriously
    cancelled. Checked by looking for a live timer that is neither cancelled nor
    fired -- counting threads would race with everything else in the process.
    """
    q.run(session, "SELECT 1")
    with pytest.raises(q.QueryRefused):
        q.run(session, "SELECT * FROM marts.nope")
    with pytest.raises(q.QueryRefused):
        q.run(session, "SELECT count(*) FROM range(100000000000) t(i)", timeout=0.3)

    pending = [
        thread for thread in threading.enumerate()
        if isinstance(thread, threading.Timer) and not thread.finished.is_set()
    ]
    assert pending == []


# --------------------------------------------------------------------------------
# the write switch
# --------------------------------------------------------------------------------


def test_writes_are_shut_until_the_deployment_releases_them(session, monkeypatch):
    """Phase 4 first: a raw partition the sources no longer hold cannot be rebuilt."""
    monkeypatch.delenv("DATALAKE_API_ALLOW_WRITES", raising=False)
    assert q.writes_are_released() is False
    with pytest.raises(q.QueryRefused, match="writes are not enabled"):
        q.run(session, "CREATE TABLE t AS SELECT 1", read_only=False)


def test_a_released_deployment_still_needs_a_writable_session(session, monkeypatch):
    monkeypatch.setenv("DATALAKE_API_ALLOW_WRITES", "yes")
    with pytest.raises(q.QueryRefused, match="built read-only"):
        q.run(session, "CREATE TABLE t AS SELECT 1", read_only=False)


def test_a_write_runs_when_both_are_true_and_says_the_warehouse_is_now_stale(lake, monkeypatch):
    monkeypatch.setenv("DATALAKE_API_ALLOW_WRITES", "yes")
    with q.LakeSession(lake=lake, writable=True) as writable:
        result = q.run(writable, "CREATE TABLE scratch AS SELECT 1 AS a", read_only=False)
    assert result.statement_type == "CREATE"
    assert any("rebuild it" in note for note in result.notes)


def test_a_read_only_switch_is_on_unless_it_is_literally_false(lake, monkeypatch):
    """The direction a mistake falls in is the whole point of the default."""
    monkeypatch.setenv("DATALAKE_API_ALLOW_WRITES", "yes")
    client = _client(lake)
    for body in ({"sql": "CREATE TABLE t AS SELECT 1"},
                 {"sql": "CREATE TABLE t AS SELECT 1", "read_only": None},
                 {"sql": "CREATE TABLE t AS SELECT 1", "read_only": "false"}):
        response = client.post("/query", json=body)
        assert response.status_code == 400
        assert "read-only switch is on" in response.json()["detail"]


# --------------------------------------------------------------------------------
# the audit log
# --------------------------------------------------------------------------------


def test_every_statement_is_written_down(tmp_path):
    log = AuditLog(path=str(tmp_path / "audit" / "queries.jsonl"))
    log.write(Entry(client_id="ai-platform", user="operator", sql="SELECT 1",
                    statement_type="SELECT", read_only=True, outcome="ok", rows=1))
    written = json.loads((tmp_path / "audit" / "queries.jsonl").read_text())
    assert written["user"] == "operator"
    assert written["statement_type"] == "SELECT"
    assert written["at"].endswith("+00:00")


def test_a_refusal_is_written_down_too(tmp_path):
    """The interesting half of the record: what somebody tried."""
    path = tmp_path / "queries.jsonl"
    log = AuditLog(path=str(path))
    log.write(Entry(client_id="c", user="u", sql="DELETE FROM raw.npm_access",
                    statement_type="DELETE", read_only=True, outcome="refused",
                    detail="the read-only switch is on"))
    written = json.loads(path.read_text())
    assert written["outcome"] == "refused"
    assert "DELETE" in written["sql"]


def test_the_log_rotates_and_keeps_a_bounded_number_of_generations(tmp_path):
    """The host is [rate withheld] full. An audit log must not be what fills it."""
    path = tmp_path / "queries.jsonl"
    log = AuditLog(path=str(path), max_bytes=200, generations=2)
    for index in range(40):
        log.write(Entry(client_id="c", user="u", sql=f"SELECT {index}",
                        statement_type="SELECT", read_only=True, outcome="ok"))
    generations = sorted(p.name for p in tmp_path.iterdir())
    assert generations == ["queries.jsonl", "queries.jsonl.1", "queries.jsonl.2"]


def test_an_unwritable_audit_volume_does_not_break_every_query(tmp_path, caplog):
    """It must be loud, and it must not turn a working service into a 500."""
    blocked = tmp_path / "file"
    blocked.write_text("not a directory")
    log = AuditLog(path=str(blocked / "queries.jsonl"))
    log.write(Entry(client_id="c", user="u", sql="SELECT 1",
                    statement_type="SELECT", read_only=True, outcome="ok"))
    assert "AUDIT LOG UNWRITABLE" in caplog.text


# --------------------------------------------------------------------------------
# the route
# --------------------------------------------------------------------------------


def _client(lake, audit=None) -> TestClient:
    return TestClient(create_app(sessions=q.SessionCache(lake=lake, stamp=lambda: None),
                                 audit=audit or AuditLog(path="/dev/null")))


def test_the_route_answers_a_select(lake):
    body = _client(lake).post("/query", json={"sql": "SELECT 42 AS answer"}).json()
    assert body["rows"] == [[42]]
    assert body["columns"] == ["answer"]
    assert body["statement_type"] == "SELECT"


def test_the_route_refuses_a_write_with_a_message_for_the_person_typing(lake):
    response = _client(lake).post("/query", json={"sql": "DROP VIEW marts.agg_traffic_daily"})
    assert response.status_code == 400
    assert "read-only switch is on" in response.json()["detail"]


def test_the_route_needs_sql(lake):
    assert _client(lake).post("/query", json={}).status_code == 400
    assert _client(lake).post("/query", json={"sql": 12}).status_code == 400
    assert _client(lake).post("/query", content=b"not json").status_code == 400


def test_the_route_will_not_be_talked_above_the_row_cap(lake):
    body = _client(lake).post(
        "/query", json={"sql": "SELECT i FROM range(5000) t(i)", "max_rows": 10_000_000}
    ).json()
    assert body["row_count"] == q.MAX_ROWS
    assert body["truncated"] is True


def test_the_route_rejects_a_nonsense_row_cap(lake):
    assert _client(lake).post("/query", json={"sql": "SELECT 1", "max_rows": 0}).status_code == 400
    assert _client(lake).post("/query", json={"sql": "SELECT 1", "max_rows": "10"}).status_code == 400


def test_the_route_audits_what_it_ran_and_what_it_refused(lake, tmp_path):
    path = tmp_path / "queries.jsonl"
    client = _client(lake, audit=AuditLog(path=str(path)))
    client.post("/query", json={"sql": "SELECT 1"})
    client.post("/query", json={"sql": "DELETE FROM marts.agg_traffic_daily"})

    written = [json.loads(line) for line in path.read_text().splitlines()]
    assert [(w["outcome"], w["statement_type"]) for w in written] == [
        ("ok", "SELECT"), ("refused", "DELETE")
    ]


def test_the_session_is_rebound_when_the_lake_is_republished(lake):
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    assert pool.get() is first

    stamp[0] = (2, 2)  # warehouse_serving renamed a new copy into place
    assert pool.get() is not first
    pool.close()
