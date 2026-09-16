"""The event-loop freeze measured on 2026-09-16, and the fix for it.

`datalake-api` 502'd for the length of a `SessionCache` rebuild -- 104.7s, measured,
across 168 datasets -- because `run_query`/`get_catalogue`/`refresh_catalogue` are
`async def` (needed for `await verifier.verify_token(...)`) and called the slow,
synchronous rebuild inline. Starlette only auto-threads a *sync* route function; an
async one that blocks inline blocks the event loop for every other request,
including the unauthenticated `/health` liveness check -- which is what took 11.4s
that night.

Three things are tested here, each against the actual bug rather than the API's
surface: the event loop stays responsive while a session builds or a query runs
(api/app.py wraps every blocking call in `run_in_threadpool`); a republish keeps
serving the previous session while a replacement binds in the background, and only
closes it once nothing is still using it (SessionCache's reference counting); and
`single_schema` skips the `union_by_name` footer scan that made the rebuild slow in
the first place, proven by whether a corrupt "trap" file under the same prefix is
ever opened, not by timing.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from starlette.testclient import TestClient

from homelab_datalake import layout
from homelab_datalake.api import query as q
from homelab_datalake.api.app import create_app
from homelab_datalake.api.audit import AuditLog
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def lake(tmp_path) -> LakeStorage:
    storage = LakeStorage(local_root=str(tmp_path / "lake"))
    storage.write_table(
        "marts/infrastructure/agg_traffic_daily/partition_date=2026-08-01/data.parquet",
        pa.table({"requests": pa.array([1, 2, 3])}),
    )
    return storage


def _wait_for(predicate, timeout: float = 5.0, step: float = 0.01) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(step)
    return predicate()


# --------------------------------------------------------------------------------
# the event loop stays responsive
# --------------------------------------------------------------------------------


def test_health_answers_promptly_while_a_cold_session_is_being_built(lake, monkeypatch):
    """The regression itself: a slow LakeSession._build() must not freeze the
    loop that /health also runs on. Needs a *shared* portal -- TestClient used
    without `with` gives every call its own event loop, which would pass this
    test whether or not the fix is in place; confirmed by running this same
    shape of test against a deliberately unthreaded handler.
    """
    real_build = q.LakeSession._build
    build_started = threading.Event()

    def slow_build(self):
        build_started.set()
        time.sleep(1.0)
        return real_build(self)

    monkeypatch.setattr(q.LakeSession, "_build", slow_build)
    pool = q.SessionCache(lake=lake, stamp=lambda: (1, 1))
    app = create_app(sessions=pool, audit=AuditLog(path="/dev/null"))

    with TestClient(app) as client:
        result: dict = {}

        def hit_query():
            result["response"] = client.post("/query", json={"sql": "SELECT 1"})

        thread = threading.Thread(target=hit_query)
        thread.start()
        assert build_started.wait(timeout=2.0), "the slow build never started"

        began = time.perf_counter()
        health = client.get("/health")
        elapsed = time.perf_counter() - began
        thread.join(timeout=5.0)

    assert health.status_code == 200
    assert elapsed < 0.5, f"/health took {elapsed:.2f}s while a session was building"
    assert result["response"].status_code == 200


def test_health_answers_promptly_while_a_slow_query_runs(lake, monkeypatch):
    real_run = q.run
    query_started = threading.Event()

    def slow_run(session, sql, **kwargs):
        query_started.set()
        time.sleep(1.0)
        return real_run(session, sql, **kwargs)

    monkeypatch.setattr(q, "run", slow_run)
    pool = q.SessionCache(lake=lake, stamp=lambda: (1, 1))
    app = create_app(sessions=pool, audit=AuditLog(path="/dev/null"))

    with TestClient(app) as client:
        result: dict = {}

        def hit_query():
            result["response"] = client.post("/query", json={"sql": "SELECT 1"})

        thread = threading.Thread(target=hit_query)
        thread.start()
        assert query_started.wait(timeout=2.0), "the slow query never started"

        began = time.perf_counter()
        health = client.get("/health")
        elapsed = time.perf_counter() - began
        thread.join(timeout=5.0)

    assert health.status_code == 200
    assert elapsed < 0.5, f"/health took {elapsed:.2f}s while a query was running"
    assert result["response"].status_code == 200


# --------------------------------------------------------------------------------
# a republish keeps serving the old session, and swaps atomically
# --------------------------------------------------------------------------------


def test_get_returns_immediately_from_the_old_session_while_a_rebuild_is_blocked(lake, monkeypatch):
    """`get()` must never make a caller wait for a rebind that is already
    running -- only the very first, cold-start build is allowed to block.
    """
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    pool.release(first)

    real_build = q.LakeSession._build
    rebuild_started = threading.Event()
    release_rebuild = threading.Event()

    def blocked_build(self):
        rebuild_started.set()
        release_rebuild.wait(timeout=5.0)
        return real_build(self)

    monkeypatch.setattr(q.LakeSession, "_build", blocked_build)
    stamp[0] = (2, 2)  # warehouse_serving renamed a new copy into place

    began = time.perf_counter()
    still_old = pool.get()
    elapsed = time.perf_counter() - began
    pool.release(still_old)

    assert still_old is first
    assert elapsed < 0.5, f"get() took {elapsed:.2f}s; it should never wait on a rebuild in progress"
    assert rebuild_started.wait(timeout=2.0), "get() never started the background rebuild"

    release_rebuild.set()

    def swapped() -> bool:
        current = pool.get()
        pool.release(current)
        return current is not first

    assert _wait_for(swapped), "the background rebuild never finished and swapped in"
    pool.close()


def test_the_new_session_is_served_once_the_background_rebuild_finishes(lake):
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    pool.release(first)

    stamp[0] = (2, 2)

    def swapped() -> bool:
        current = pool.get()
        pool.release(current)
        return current is not first

    assert _wait_for(swapped), "the background rebuild never swapped in a new session"
    pool.close()


def test_the_old_session_is_closed_only_after_in_flight_queries_finish(lake, monkeypatch):
    closed: list = []
    real_close = q.LakeSession.close

    def tracking_close(self):
        closed.append(self)
        real_close(self)

    monkeypatch.setattr(q.LakeSession, "close", tracking_close)

    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()  # stands in for a query still running against `first`

    stamp[0] = (2, 2)

    def swapped() -> bool:
        current = pool.get()
        pool.release(current)
        return current is not first

    assert _wait_for(swapped), "the background rebuild never swapped in a new session"

    # `first` was superseded, but this test never released the reference taken
    # above -- an in-flight query -- so it must still be open.
    assert first not in closed

    pool.release(first)  # the in-flight query finishes
    assert first in closed
    pool.close()


def test_a_failed_rebuild_leaves_the_old_session_serving(lake, monkeypatch):
    """A bad rebuild -- the lake unreachable, say -- must not take down a
    session that was working: it should log and leave the previous session in
    place, observable only through `get()` still returning it, not through the
    cache's internals.
    """
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    pool.release(first)

    rebuild_attempted = threading.Event()

    def failing_build(self):
        rebuild_attempted.set()
        raise RuntimeError("the lake is unreachable")

    monkeypatch.setattr(q.LakeSession, "_build", failing_build)
    stamp[0] = (2, 2)
    pool.maybe_warm()  # notices the stamp change and starts the (failing) rebuild

    assert rebuild_attempted.wait(timeout=2.0), "the background rebuild never ran"
    time.sleep(0.05)  # let the background thread finish handling the exception

    current = pool.get()
    pool.release(current)
    assert current is first  # the failed rebuild did not touch the serving session

    pool.close()


# --------------------------------------------------------------------------------
# proactive warming
# --------------------------------------------------------------------------------


def test_maybe_warm_starts_a_rebuild_without_blocking(lake):
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    pool.release(first)

    stamp[0] = (2, 2)
    began = time.perf_counter()
    pool.maybe_warm()
    elapsed = time.perf_counter() - began
    assert elapsed < 0.5

    def swapped() -> bool:
        current = pool.get()
        pool.release(current)
        return current is not first

    assert _wait_for(swapped)
    pool.close()


def test_maybe_warm_on_a_cold_cache_does_not_build_a_session(lake, monkeypatch):
    """Warming keeps an already-warm session warm; the cold-start build stays
    get()'s job, so a caller and the warmer do not race to build the first one.
    """
    built = []
    real_init = q.LakeSession.__init__

    def counting_init(self, *args, **kwargs):
        built.append(1)
        return real_init(self, *args, **kwargs)

    monkeypatch.setattr(q.LakeSession, "__init__", counting_init)
    pool = q.SessionCache(lake=lake, stamp=lambda: (1, 1))

    pool.maybe_warm()
    time.sleep(0.05)
    assert built == []
    pool.close()


def test_background_warming_notices_a_republish_without_any_query(lake):
    """So the 05:00-08:00 automated reports find a warm session even if nothing
    queried the lake between the nightly publish and their first question."""
    stamp = [(1, 1)]
    pool = q.SessionCache(lake=lake, stamp=lambda: stamp[0])
    first = pool.get()
    pool.release(first)

    pool.start_background_warming(interval=0.02)
    stamp[0] = (2, 2)

    def swapped() -> bool:
        current = pool.get()
        pool.release(current)
        return current is not first

    assert _wait_for(swapped), "the background warmer never noticed the stamp change"
    pool.close()


# --------------------------------------------------------------------------------
# single_schema: binds without reading every file's footer
# --------------------------------------------------------------------------------


def _fixture_dataset(table: str, name: str, single_schema: bool) -> layout.Dataset:
    return layout.Dataset(
        table=table, layer=layout.LAYER_RAW, domain="test", source="test",
        name=name, description="", single_schema=single_schema,
    )


def _land_with_a_corrupt_trap(lake: LakeStorage, dataset: layout.Dataset, tmp_path) -> None:
    schema = pa.schema([("id", pa.int64()), ("val", pa.string())])
    for i in range(3):
        lake.write_table(
            f"{dataset.prefix}/p{i}.parquet",
            pa.table(
                {"id": pa.array([i], pa.int64()), "val": pa.array([f"v{i}"], pa.string())},
                schema=schema,
            ),
        )
    # A file that matches the glob but is not valid Parquet. union_by_name opens
    # every matching file's footer before CREATE VIEW returns, so this makes
    # that path fail; a bind that never touches it (single_schema) is unaffected.
    trap = Path(tmp_path) / dataset.prefix / "zz_corrupt.parquet"
    trap.parent.mkdir(parents=True, exist_ok=True)
    trap.write_bytes(b"not a parquet file, just padding bytes to look like one")


def test_a_single_schema_view_binds_without_touching_every_file(tmp_path):
    """The bind itself -- CREATE VIEW -- must not open the corrupt trap file.

    This checks bind time only, deliberately: an actual `SELECT` against this
    view still has to read every matched file's data, corrupt trap included,
    the same as it always would -- single_schema changes when the footers are
    read (at bind, for every file, vs. lazily, per file actually scanned), not
    whether they ever are. Correctness of the data path (no trap) is covered
    by test_a_single_schema_dataset_returns_correct_data_through_a_real_session.
    """
    lake = LakeStorage(local_root=str(tmp_path))
    dataset = _fixture_dataset("fixture_single_schema", "fixture_a", single_schema=True)
    _land_with_a_corrupt_trap(lake, dataset, tmp_path)

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {dataset.layer};")
        con.execute(q.view_create_statement(lake, dataset))  # must not raise
    finally:
        con.close()


def test_without_single_schema_the_identical_fixture_fails_to_bind(tmp_path):
    """Proves the trap above is real: the same fixture, read the default
    (union_by_name) way, does fail -- which is the cost single_schema exists to
    avoid where it is safe to.
    """
    lake = LakeStorage(local_root=str(tmp_path))
    dataset = _fixture_dataset("fixture_default_schema", "fixture_b", single_schema=False)
    _land_with_a_corrupt_trap(lake, dataset, tmp_path)

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {dataset.layer};")
        with pytest.raises(duckdb.Error):
            con.execute(q.view_create_statement(lake, dataset))
    finally:
        con.close()


def test_a_single_schema_dataset_returns_correct_data_through_a_real_session(tmp_path):
    """Correctness, not just bind cost: the flagged dataset still answers
    right, using the exact schema constant the real writer uses.
    """
    from homelab_datalake.assets.jellyfin import _PLAYBACK_SCHEMA

    lake = LakeStorage(local_root=str(tmp_path))
    dataset = layout.dataset("jellyfin_playback")
    assert dataset.single_schema is True

    row = {
        "partition_hour": datetime(2026, 9, 1, 12, tzinfo=UTC),
        "dt": date(2026, 9, 1),
        "_extracted_at": datetime(2026, 9, 1, 12, 5, tzinfo=UTC),
        "started_at": datetime(2026, 9, 1, 12, 0, tzinfo=UTC),
        "user_id": "u1", "user_name": "greg", "item_id": "i1",
        "item_type": "Movie", "item_name": "Example", "series_name": None,
        "playback_method": "DirectPlay", "client_name": "Jellyfin Web",
        "device_name": "desktop", "play_duration_seconds": 3600,
    }
    table = pa.Table.from_pylist([row], schema=_PLAYBACK_SCHEMA)
    lake.write_table(
        f"{dataset.prefix}/partition_hour=2026-09-01T12:00:00/data.parquet", table
    )

    with q.LakeSession(lake=lake) as session:
        result = q.run(
            session, "SELECT user_name, play_duration_seconds FROM raw.jellyfin_playback"
        )
    assert result.rows == [["greg", 3600]]
