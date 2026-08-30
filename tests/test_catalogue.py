"""The catalogue: what it reads, what it joins, and what it does when either is missing.

The expensive failure this guards against is not an exception. It is a catalogue
that answers cheerfully with the wrong columns -- because the schema came from YAML
rather than from the data, or because one dataset's objects were counted under its
neighbour's prefix. Both look like a working autocomplete right up to the query that
fails.
"""

import json
from datetime import date

import pyarrow as pa
import pytest
from starlette.testclient import TestClient

from homelab_datalake import layout
from homelab_datalake.api import catalogue as cat
from homelab_datalake.api.app import USER_HEADER, create_app
from homelab_datalake.resources import LakeStorage


@pytest.fixture
def lake(tmp_path) -> LakeStorage:
    return LakeStorage(local_root=str(tmp_path))


def _land(lake: LakeStorage, table: str, partition: str | None, **columns) -> None:
    """Write one Parquet object where the dataset's real prefix says it goes.

    The partition column is written *into* the object as well as into the path,
    because that is what the publisher does -- see homelab_datalake.layout. A fixture
    that left it out would let DuckDB synthesise it from the directory name, and the
    test would then be checking a shape production never has.
    """
    dataset = layout.dataset(table)
    key = f"{dataset.prefix}/data.parquet"
    if partition is not None:
        column = dataset.partition_column
        key = f"{dataset.prefix}/{column}={partition}/data.parquet"
        height = len(next(iter(columns.values())))
        columns = {
            column: pa.array([date.fromisoformat(partition)] * height, pa.date32()),
            **columns,
        }
    lake.write_table(key, pa.table(columns))


def _manifest(tmp_path, nodes) -> str:
    """A manifest with only the fields the catalogue reads."""
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(nodes))
    return str(path)


# --------------------------------------------------------------------------------
# the footers
# --------------------------------------------------------------------------------


def test_columns_and_types_come_from_the_parquet(lake, tmp_path):
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200, 404], pa.int32()),
          request_host=pa.array(["a", "b"]))

    built = cat.build(lake=lake, manifest_path=str(tmp_path / "absent.json"))
    entry = {t.table: t for t in built.tables}["npm_access"]

    assert [(c.name, c.type) for c in entry.columns] == [
        ("dt", "DATE"), ("status", "INTEGER"), ("request_host", "VARCHAR")
    ]
    assert entry.rows == 2
    assert entry.objects == 1
    assert entry.bytes > 0
    assert entry.unavailable is None


def test_a_column_added_later_still_appears(lake, tmp_path):
    """Schema evolution is a supported state, so the catalogue describes the union.

    The loader reads these partitions with union_by_name; a catalogue that described
    whichever object DuckDB opened first would offer an autocomplete missing the
    newest column, or one offering a column the reader cannot find in older days.
    """
    _land(lake, "camera_events", "2026-08-01", event_id=pa.array(["a"]))
    _land(lake, "camera_events", "2026-08-02", event_id=pa.array(["b"]),
          confidence=pa.array([0.5]))

    entry = _entry(cat.build(lake=lake, manifest_path=str(tmp_path / "x.json")), "camera_events")
    assert {c.name for c in entry.columns} == {"dt", "event_id", "confidence"}
    assert entry.rows == 2


def test_a_dataset_with_nothing_landed_is_listed_and_says_so(lake, tmp_path):
    """Not dropped: an empty dataset is a fact, and a silently short tree is a bug."""
    built = cat.build(lake=lake, manifest_path=str(tmp_path / "x.json"))

    assert len(built.tables) == len(layout.ALL_DATASETS)
    entry = _entry(built, "tmdb_titles")
    assert entry.columns == ()
    assert entry.rows == 0
    assert "no objects" in entry.unavailable


def test_objects_are_not_counted_under_a_neighbouring_prefix(lake, tmp_path):
    """`raw/.../npm/access` is a string prefix of `raw/.../npm/access_rejects`.

    Only the trailing separator separates them, which is the same trap the layout
    migration had to be written around.
    """
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    _land(lake, "npm_access_rejects", "2026-08-01", raw_line=pa.array(["x"]))
    _land(lake, "npm_access_rejects", "2026-08-02", raw_line=pa.array(["y"]))

    built = cat.build(lake=lake, manifest_path=str(tmp_path / "x.json"))
    assert _entry(built, "npm_access").objects == 1
    assert _entry(built, "npm_access_rejects").objects == 2


# --------------------------------------------------------------------------------
# the manifest
# --------------------------------------------------------------------------------


def test_descriptions_are_joined_onto_the_columns_that_exist(lake, tmp_path):
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]), request_host=pa.array(["a"]))
    path = _manifest(tmp_path, {
        "sources": {
            "source.pkg.raw.npm_access": {
                "resource_type": "source", "name": "npm_access",
                "description": "landed requests",
                "columns": {"status": {"description": "The HTTP status\n  code returned."}},
            }
        },
        "nodes": {},
    })

    entry = _entry(cat.build(lake=lake, manifest_path=path), "npm_access")
    described = {c.name: c.description for c in entry.columns}
    # The YAML line wrapping is folded away: a description rendered in a table cell
    # should not carry the indentation of the file it was written in.
    assert described["status"] == "The HTTP status code returned."
    assert described["request_host"] == ""


def test_documentation_for_a_column_that_does_not_exist_is_not_invented(lake, tmp_path):
    """A rename done on one side only. The data wins; the stale doc is dropped."""
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    path = _manifest(tmp_path, {
        "sources": {
            "source.pkg.raw.npm_access": {
                "resource_type": "source", "name": "npm_access",
                "columns": {"http_status": {"description": "the old name"}},
            }
        },
        "nodes": {},
    })

    entry = _entry(cat.build(lake=lake, manifest_path=path), "npm_access")
    assert [c.name for c in entry.columns] == ["dt", "status"]


def test_a_missing_manifest_leaves_blanks_rather_than_failing(lake, tmp_path):
    """The catalogue's job is to say what exists. It can do that with no prose."""
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))

    built = cat.build(lake=lake, manifest_path=str(tmp_path / "not-there.json"))
    assert built.documented is False
    assert _entry(built, "npm_access").columns[0].description == ""


def test_the_registry_description_wins_over_the_manifest(lake, tmp_path):
    """The tree and the Dagster graph must not disagree about what a dataset is."""
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    path = _manifest(tmp_path, {
        "sources": {
            "s": {"resource_type": "source", "name": "npm_access",
                  "description": "something else", "columns": {}}
        },
        "nodes": {},
    })

    entry = _entry(cat.build(lake=lake, manifest_path=path), "npm_access")
    assert entry.description == layout.dataset("npm_access").description


def test_model_descriptions_are_read_too(lake, tmp_path):
    _land(lake, "agg_traffic_daily", "2026-08-01", requests=pa.array([1]))
    path = _manifest(tmp_path, {
        "sources": {},
        "nodes": {
            "model.pkg.agg_traffic_daily": {
                "resource_type": "model", "name": "agg_traffic_daily",
                "columns": {"requests": {"description": "How many."}},
            },
            "test.pkg.not_null_x": {"resource_type": "test", "name": "requests"},
        },
    })

    entry = _entry(cat.build(lake=lake, manifest_path=path), "agg_traffic_daily")
    assert {c.name: c.description for c in entry.columns}["requests"] == "How many."


def test_real_manifest_parses_if_it_is_there():
    """A guard on the shape, not on the content -- the file is a build artefact."""
    docs = cat.read_manifest()
    if not docs:
        pytest.skip("no dbt manifest built in this checkout")
    assert "fct_http_request" in docs
    assert isinstance(docs["fct_http_request"].columns, dict)


# --------------------------------------------------------------------------------
# the shape of an entry
# --------------------------------------------------------------------------------


def test_every_dataset_reports_where_it_lives_and_what_writes_it():
    """The workbench deep-links from these. A guessed asset key links to nothing."""
    for dataset in layout.ALL_DATASETS:
        path = cat.asset_path(dataset)
        if dataset.layer == layout.LAYER_RAW:
            assert path[0].endswith("_raw"), dataset.table
        else:
            assert path == ("lake", dataset.layer, dataset.table)


def test_entries_serialise_to_plain_json(lake, tmp_path):
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    payload = cat.build(lake=lake, manifest_path=str(tmp_path / "x.json")).as_dict()
    json.dumps(payload)  # would raise on a dataclass or a tuple of them
    assert payload["columns_total"] == 2  # dt and status
    assert payload["columns_described"] == 0


# --------------------------------------------------------------------------------
# the cache
# --------------------------------------------------------------------------------


def test_the_catalogue_is_built_once_and_reused():
    """25 footer reads per keystroke of an autocomplete is the thing to avoid."""
    builds = []

    def builder():
        builds.append(1)
        return cat.Catalogue(tables=(), built_at=0.0, build_ms=0.0, lake="test", documented=True)

    cache = cat.CatalogueCache(builder=builder)
    cache.get()
    cache.get()
    assert len(builds) == 1

    cache.refresh()
    assert len(builds) == 2


def test_the_cache_expires_as_a_backstop():
    """The publish stamp keeps it current; the timer is for a deployment that has none."""
    builds = []

    def builder():
        builds.append(1)
        return cat.Catalogue(tables=(), built_at=0.0, build_ms=0.0, lake="test", documented=True)

    cache = cat.CatalogueCache(ttl=-1.0, builder=builder, stamp=lambda: None)
    cache.get()
    cache.get()
    assert len(builds) == 2


def test_a_republished_lake_invalidates_the_catalogue():
    """The nightly job swaps the serving file last. That swap is the signal."""
    builds = []
    stamp = [(1, 100)]

    def builder():
        builds.append(1)
        return cat.Catalogue(tables=(), built_at=0.0, build_ms=0.0, lake="test", documented=True)

    cache = cat.CatalogueCache(builder=builder, stamp=lambda: stamp[0])
    cache.get()
    cache.get()
    assert len(builds) == 1

    stamp[0] = (2, 200)  # renamed into place by warehouse_serving
    cache.get()
    assert len(builds) == 2


def test_the_publish_stamp_changes_only_when_the_file_does(tmp_path):
    path = tmp_path / "serving.duckdb"
    assert cat.publish_stamp(str(path)) is None

    path.write_bytes(b"first")
    first = cat.publish_stamp(str(path))
    assert first is not None
    assert cat.publish_stamp(str(path)) == first

    # warehouse_serving writes a temporary file and renames it into place, so the
    # inode changes -- a same-second rename must still be noticed.
    other = tmp_path / "serving.duckdb.building"
    other.write_bytes(b"second")
    other.replace(path)
    assert cat.publish_stamp(str(path)) != first


# --------------------------------------------------------------------------------
# the routes
# --------------------------------------------------------------------------------


@pytest.fixture
def client(lake, tmp_path):
    cache = cat.CatalogueCache(
        builder=lambda: cat.build(lake=lake, manifest_path=str(tmp_path / "x.json"))
    )
    return TestClient(create_app(catalogue=cache))


def test_health_needs_no_token(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_catalogue_is_served_as_json(client, lake):
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    body = client.get("/catalogue").json()
    assert len(body["tables"]) == len(layout.ALL_DATASETS)


def test_refresh_rereads_the_lake(client, lake):
    assert client.get("/catalogue").json()["columns_total"] == 0
    _land(lake, "npm_access", "2026-08-01", status=pa.array([200]))
    # Still cached: the point of the cache is that this does not re-read.
    assert client.get("/catalogue").json()["columns_total"] == 0
    assert client.post("/catalogue/refresh").json()["columns_total"] == 2


def test_a_request_without_a_token_is_refused_when_a_verifier_is_configured(lake, tmp_path):
    class Rejecting:
        issuer = "https://keycloak.example/realms/test"

        async def verify_token(self, token):
            return None

    cache = cat.CatalogueCache(
        builder=lambda: cat.build(lake=lake, manifest_path=str(tmp_path / "x.json"))
    )
    client = TestClient(create_app(catalogue=cache, verifier=Rejecting()))

    assert client.get("/catalogue").status_code == 401
    assert client.get("/catalogue", headers={"Authorization": "Bearer nope"}).status_code == 401
    # Liveness stays open, so a restart loop is diagnosable without a token.
    assert client.get("/health").status_code == 200


def test_the_gateway_names_the_person_behind_the_request(lake, tmp_path):
    """The token names a service account. The audit log has to name a human."""
    seen = []

    class Accepting:
        issuer = "https://keycloak.example/realms/test"

        async def verify_token(self, token):
            class Access:
                client_id = "ai-platform"
            return Access()

    from homelab_datalake.api import app as app_module

    original = app_module.authenticate

    async def spy(request, verifier):
        caller = await original(request, verifier)
        seen.append(caller)
        return caller

    app_module.authenticate = spy
    try:
        cache = cat.CatalogueCache(
            builder=lambda: cat.build(lake=lake, manifest_path=str(tmp_path / "x.json"))
        )
        client = TestClient(create_app(catalogue=cache, verifier=Accepting()))
        client.get("/catalogue", headers={"Authorization": "Bearer ok", USER_HEADER: "operator"})
    finally:
        app_module.authenticate = original

    assert seen[0].client_id == "ai-platform"
    assert seen[0].user == "operator"


def _entry(built: cat.Catalogue, table: str) -> cat.Table:
    return next(t for t in built.tables if t.table == table)
