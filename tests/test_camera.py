"""Camera extraction tests. Payloads are injected, so no camera is needed."""

import json
from datetime import UTC, date, datetime

from homelab_datalake.assets.camera import _EVENT_SCHEMA, _merge, _project
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


def test_undeclared_fields_are_captured_rather_than_dropped():
    """A field the export starts sending is kept, in `_unmapped`, and reported.

    This is the case that cost a month of `counts`: the projection dropped anything
    it did not declare and said nothing, so a field arriving on every row was
    indistinguishable from a field that never existed. The column set stays fixed --
    the warehouse is not widened by an upstream change -- but the value survives and
    the caller is told which key it was.
    """
    rows, unmapped = _project(
        [event(shapes=[{"points": [1, 2]}], playlistUrl="/stream/x.m3u8")], _EVENT_SCHEMA
    )
    assert set(rows[0]) == {f.name for f in _EVENT_SCHEMA}
    assert unmapped == {"shapes", "playlistUrl"}
    kept = json.loads(rows[0]["_unmapped"])
    assert kept["playlistUrl"] == "/stream/x.m3u8"
    assert kept["shapes"] == [{"points": [1, 2]}]


def test_an_unknown_key_inside_a_declared_struct_is_captured_too():
    """A new subject would otherwise fail the whole partition, or vanish."""
    rows, unmapped = _project(
        [event(counts={"person": 1, "vehicle": 0, "animal": 0, "other": 0, "drone": 3})],
        _EVENT_SCHEMA,
    )
    assert rows[0]["counts"]["person"] == 1
    assert unmapped == {"counts.drone"}
    assert json.loads(rows[0]["_unmapped"]) == {"counts.drone": 3}


def test_a_row_with_nothing_unexpected_has_no_unmapped_payload():
    """`_unmapped` is a signal, so it must be quiet in the ordinary case."""
    rows, unmapped = _project([event()], _EVENT_SCHEMA)
    assert unmapped == set()
    assert rows[0]["_unmapped"] is None


def test_counts_are_kept():
    """The subject breakdown every graph of this data is built from."""
    rows, _ = _project(
        [event(counts={"person": 2, "vehicle": 1, "animal": 0, "other": 0})], _EVENT_SCHEMA
    )
    assert rows[0]["counts"] == {"person": 2, "vehicle": 1, "animal": 0, "other": 0}


def test_missing_declared_fields_become_null_not_missing():
    """Arrow needs every declared key present; a sparse row must not shift columns."""
    rows, _ = _project([{"id": "e1"}], _EVENT_SCHEMA)
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


# --------------------------------------------------------------------------------
# merging: the lake accumulates, the source only offers
# --------------------------------------------------------------------------------


def _stamped(rows, at):
    """Project rows as one read would have landed them, stamped at `at`."""
    projected, _ = _project([{**r, "dt": DAY, "_extracted_at": at} for r in rows], _EVENT_SCHEMA)
    return projected


def test_a_reread_does_not_delete_events_the_camera_has_evicted():
    """The failure this whole design exists to prevent.

    The camera's store is capped at [count withheld] events and drops the oldest, so re-reading
    an old day legitimately returns fewer rows than the lake holds. Measured on
    2026-08-31, partition 2026-08-08 held [count withheld] events and the export offered 287. A
    partition that replaced would have destroyed [count withheld] events that exist nowhere else.
    """
    first_read = datetime(2026, 8, 8, 4, 0, tzinfo=UTC)
    second_read = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)

    in_lake = _stamped([event(id=f"e{n}") for n in range(10)], first_read)
    still_offered = _stamped([event(id=f"e{n}") for n in range(3)], second_read)

    merged, retained = _merge(in_lake, still_offered, _EVENT_SCHEMA)

    assert len(merged) == 10, "the merge lost events the source no longer offers"
    assert retained == 7
    assert {row["id"] for row in merged} == {f"e{n}" for n in range(10)}


def test_a_later_version_of_an_event_wins():
    """Late-arriving analysis must still overwrite the candidate it replaces."""
    early = _stamped(
        [event(id="e1", lifecycle="candidate", confidence=None, summary=None)],
        datetime(2026, 8, 27, 6, 0, tzinfo=UTC),
    )
    late = _stamped([event(id="e1", summary="Analysis landed.")],
                    datetime(2026, 8, 27, 9, 0, tzinfo=UTC))

    merged, retained = _merge(early, late, _EVENT_SCHEMA)

    assert len(merged) == 1 and retained == 0
    assert merged[0]["lifecycle"] == "confirmed"
    assert merged[0]["summary"] == "Analysis landed."


def test_an_analysed_version_beats_an_unanalysed_one_at_the_same_instant():
    """Two reads can share a microsecond; the tie must not go to the candidate."""
    same = datetime(2026, 8, 27, 6, 0, tzinfo=UTC)
    candidate = _stamped([event(id="e1", lifecycle="candidate", summary=None)], same)
    confirmed = _stamped([event(id="e1", summary="Analysed.")], same)

    assert _merge(candidate, confirmed, _EVENT_SCHEMA)[0][0]["lifecycle"] == "confirmed"
    assert _merge(confirmed, candidate, _EVENT_SCHEMA)[0][0]["lifecycle"] == "confirmed"


def test_rows_written_before_a_column_existed_survive_the_merge():
    """A partition landed under the old schema is missing every column added since."""
    old_shape = [{"id": "e1", "dt": DAY, "lifecycle": "confirmed",
                  "_extracted_at": datetime(2026, 8, 8, 4, 0, tzinfo=UTC)}]
    fresh = _stamped([event(id="e2", counts={"person": 1, "vehicle": 0,
                                             "animal": 0, "other": 0})],
                     datetime(2026, 8, 31, 12, 0, tzinfo=UTC))

    merged, _ = _merge(old_shape, fresh, _EVENT_SCHEMA)
    by_id = {row["id"]: row for row in merged}

    assert len(merged) == 2
    assert by_id["e1"]["counts"] is None, "an absent column must read as null, not vanish"
    assert set(by_id["e1"]) == {f.name for f in _EVENT_SCHEMA}
    assert by_id["e2"]["counts"]["person"] == 1
