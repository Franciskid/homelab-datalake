"""The join graph served to callers who would otherwise guess.

These assert the shape of the graph and, more importantly, where it comes from: edges
are read from declared `relationships` tests and nowhere else. An edge inferred by
matching column names would pair `raw.camera_events.id` with `raw.camera_clips.id`,
which are different things that happen to share a name -- and would carry none of the
enforcement that makes a declared edge trustworthy.
"""

import json

from homelab_datalake.api import relations

MANIFEST = {
    "metadata": {"generated_at": "2026-08-31T17:00:00Z"},
    "nodes": {
        "model.p.fct_camera_event": {
            "resource_type": "model", "name": "fct_camera_event", "schema": "main_marts",
            "description": "One row per camera event.",
            "primary_key": ["event_id"],
            "columns": {
                "event_id": {"description": "The event's stable id."},
                "clip_id": {"description": "The recording attached to it."},
                "happened_at": {"description": "When it happened, to the microsecond."},
            },
            "depends_on": {"nodes": ["model.p.int_camera_events"]},
        },
        "model.p.fct_camera_clip": {
            "resource_type": "model", "name": "fct_camera_clip", "schema": "main_marts",
            "description": "One row per clip.", "primary_key": ["clip_id"],
            "depends_on": {"nodes": ["model.p.stg_camera_clips"]},
        },
        "test.p.rel_1": {
            "resource_type": "test", "name": "rel_1",
            "attached_node": "model.p.fct_camera_event",
            "test_metadata": {
                "name": "relationships",
                "kwargs": {"column_name": "clip_id", "to": "ref('fct_camera_clip')",
                           "field": "clip_id"},
            },
        },
        "test.p.unique_1": {
            "resource_type": "test", "name": "unique_1",
            "attached_node": "model.p.fct_camera_event",
            "test_metadata": {"name": "unique", "kwargs": {"column_name": "event_id"}},
        },
    },
}


def _build(tmp_path, manifest=MANIFEST):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return relations.build(str(path))


def test_a_declared_foreign_key_becomes_an_edge(tmp_path):
    graph = _build(tmp_path)
    assert graph["edges"] == [{
        "from_table": "fct_camera_event", "from_column": "clip_id",
        "to_table": "fct_camera_clip", "to_column": "clip_id",
        "join_sql": "fct_camera_event.clip_id = fct_camera_clip.clip_id",
    }]


def test_the_on_clause_is_actually_in_the_payload(tmp_path):
    """The tool description promises it. `asdict` does not serialise properties, so
    for a while it promised it and shipped four bare names instead."""
    graph = _build(tmp_path)
    assert graph["edges"][0]["join_sql"]
    tables = {t["name"]: t for t in graph["tables"]}
    assert tables["fct_camera_event"]["references"][0]["join_sql"]
    assert tables["fct_camera_clip"]["referenced_by"][0]["join_sql"]


def test_the_table_name_given_is_the_one_that_binds(tmp_path):
    """dbt's build schema is `main_marts`; the database a caller queries has `marts`.

    Reporting the build name hands out a reference that raises a Catalog Error --
    which is what happened, cost a step, and ended with the query being abandoned.
    """
    tables = {t["name"]: t for t in _build(tmp_path)["tables"]}
    assert tables["fct_camera_event"]["query_as"] == "marts.fct_camera_event"
    assert tables["fct_camera_event"]["schema"] == "main_marts"


def test_documented_columns_travel_with_the_table(tmp_path):
    tables = {t["name"]: t for t in _build(tmp_path)["tables"]}
    names = [c["name"] for c in tables["fct_camera_event"]["columns"]]
    assert "clip_id" in names


def test_only_relationships_tests_become_edges(tmp_path):
    """A unique test says a column is a key, not that it points anywhere."""
    graph = _build(tmp_path)
    assert len(graph["edges"]) == 1


def test_an_edge_is_reachable_from_both_ends(tmp_path):
    """A caller holding either table must be able to find the join without scanning."""
    tables = {t["name"]: t for t in _build(tmp_path)["tables"]}
    assert tables["fct_camera_event"]["references"][0]["to_table"] == "fct_camera_clip"
    assert tables["fct_camera_clip"]["referenced_by"][0]["from_table"] == "fct_camera_event"
    assert tables["fct_camera_event"]["referenced_by"] == []


def test_the_join_clause_is_spelled_out(tmp_path):
    """So the caller writes the join rather than assembling it from four fields."""
    edge = relations.Edge("fct_camera_event", "clip_id", "fct_camera_clip", "clip_id")
    assert edge.join_sql == "fct_camera_event.clip_id = fct_camera_clip.clip_id"


def test_primary_keys_and_lineage_travel(tmp_path):
    tables = {t["name"]: t for t in _build(tmp_path)["tables"]}
    assert tables["fct_camera_event"]["primary_key"] == ["event_id"]
    assert tables["fct_camera_event"]["built_from"] == ["int_camera_events"]


def test_a_missing_manifest_is_an_empty_graph_not_a_crash(tmp_path):
    """The endpoint saying "I know of no relations" beats a 500."""
    graph = relations.build(str(tmp_path / "absent.json"))
    assert graph == {"tables": [], "edges": [], "manifest": None}


def test_a_malformed_relationship_is_skipped_rather_than_half_built(tmp_path):
    manifest = json.loads(json.dumps(MANIFEST))
    manifest["nodes"]["test.p.rel_1"]["test_metadata"]["kwargs"].pop("field")
    assert _build(tmp_path, manifest)["edges"] == []
