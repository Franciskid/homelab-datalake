"""Camera extraction tests. Payloads are injected, so no camera is needed."""

from datetime import date, datetime

from homelab_datalake.assets.camera import _EVENT_SCHEMA, _project
from homelab_datalake.parsing.camera import extract_day

DAY = date(2026, 8, 27)


def event(**overrides):
    base = {
        "id": "attention-event-1",
        "cameraId": "indoor-camera",
        "zoneId": "zone-1",
        "zoneName": "Animaux",
        "ruleType": "motion",
        "lifecycle": "confirmed",
        "startedAt": "2026-08-27T05:43:41.861Z",
        "completedAt": "2026-08-27T05:45:01.033Z",
        "confidence": 0.98,
        "summary": "Mouvement détecté.",
    }
    base.update(overrides)
    return base


def test_every_row_is_stamped_with_when_this_pipeline_saw_it():
    """_extracted_at is what lets the mart pick the newest version of an event whose
    analysis landed after the first read."""
    out = extract_day(DAY, payloads={"events": [event()], "clips": [], "logs": []})
    assert out.events[0]["_extracted_at"] == out.extracted_at
    assert out.events[0]["dt"] == DAY
    assert isinstance(out.extracted_at, datetime) and out.extracted_at.tzinfo is not None


def test_a_later_read_of_the_same_event_carries_a_later_stamp():
    """The late-arrival case: first read is a candidate, the second is analysed."""
    first = extract_day(DAY, payloads={
        "events": [event(lifecycle="candidate", confidence=None, summary=None)],
        "clips": [], "logs": []})
    second = extract_day(DAY, payloads={"events": [event()], "clips": [], "logs": []})
    assert first.events[0]["id"] == second.events[0]["id"]
    assert second.events[0]["_extracted_at"] >= first.events[0]["_extracted_at"]
    assert first.events[0]["lifecycle"] == "candidate"
    assert second.events[0]["lifecycle"] == "confirmed"


def test_undeclared_fields_are_dropped_rather_than_widening_the_warehouse():
    """A field the export starts sending is ignored until it is declared here."""
    rows = _project([event(shapes=[{"points": [1, 2]}], playlistUrl="/stream/x.m3u8")], _EVENT_SCHEMA)
    assert "shapes" not in rows[0] and "playlistUrl" not in rows[0]
    assert set(rows[0]) == {f.name for f in _EVENT_SCHEMA}


def test_missing_declared_fields_become_null_not_missing():
    """Arrow needs every declared key present; a sparse row must not shift columns."""
    rows = _project([{"id": "e1"}], _EVENT_SCHEMA)
    assert rows[0]["id"] == "e1"
    assert rows[0]["confidence"] is None and rows[0]["zoneName"] is None
    assert set(rows[0]) == {f.name for f in _EVENT_SCHEMA}


def test_null_confidence_survives_as_null():
    """The model declining to commit is a different fact from a low score."""
    out = extract_day(DAY, payloads={"events": [event(confidence=None)], "clips": [], "logs": []})
    assert out.events[0]["confidence"] is None


def test_all_three_endpoints_are_extracted():
    out = extract_day(DAY, payloads={
        "events": [event()],
        "clips": [{"id": "clip-1", "cameraId": "indoor-camera"}],
        "logs": [{"id": "log-1", "type": "analysis_completed"}],
    })
    assert len(out.events) == 1 and len(out.clips) == 1 and len(out.logs) == 1
    assert out.clips[0]["dt"] == DAY and out.logs[0]["dt"] == DAY


def test_an_empty_day_is_legal():
    out = extract_day(DAY, payloads={"events": [], "clips": [], "logs": []})
    assert out.events == [] and out.clips == [] and out.logs == []
