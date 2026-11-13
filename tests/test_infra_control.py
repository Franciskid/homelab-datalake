"""infra-control's export into the lake: rows map field for field and fit the declared
schemas, and the change log is read whole through its cursor."""

from datetime import UTC, date, datetime

import pyarrow as pa

from homelab_datalake.assets import infra_control

DAY = date(2026, 11, 13)


def test_decisions_keep_every_field_and_fit_the_schema():
    rows = infra_control.decision_rows({"decisions": [
        {"id": "a", "cidr": "203.0.113.7/32", "action": "block", "source": "auto", "reason": "Auto-blocked: score 95",
         "score": 95, "createdBy": "francois", "createdAt": "2026-11-13T09:00:00.000Z", "expiresAt": "2026-12-13T09:00:00.000Z",
         "revokedAt": None, "revokedBy": None, "via": "infra-control"},
        {"id": "b", "cidr": "198.51.100.0/24", "action": "allow", "source": "manual", "createdAt": "2026-11-12T10:00:00Z",
         "revokedAt": "2026-11-12T11:00:00Z", "revokedBy": "francois", "via": ""},
    ]}, DAY)
    table = pa.Table.from_pylist(rows, schema=infra_control.DECISION_SCHEMA)
    assert table.num_rows == 2
    assert rows[0]["created_at"] == datetime(2026, 11, 13, 9, tzinfo=UTC)
    assert rows[1]["via"] is None and rows[1]["revoked_by"] == "francois"


def test_settings_and_changes_carry_who_and_through_what():
    settings = infra_control.setting_rows({"settings": [
        {"key": "edge.autoblock.min_score", "type": "integer", "value": 80, "default": 70, "tier": "critical",
         "managedBy": "control", "origin": "override", "ai": {"writable": True}, "updatedBy": "francois",
         "updatedAt": "2026-11-13T09:00:00.000Z"},
        {"key": "server.wan_lockdown.ports", "type": "list", "value": [5057, 8097], "default": [], "tier": "critical",
         "managedBy": "ansible", "origin": "deploy", "ai": {"writable": False}, "updatedBy": None, "updatedAt": None},
    ]}, DAY)
    pa.Table.from_pylist(settings, schema=infra_control.SETTING_SCHEMA)
    assert settings[0]["value_json"] == "80" and settings[1]["value_json"] == "[5057, 8097]"
    changes = infra_control.change_rows({"changes": [
        {"id": 7, "key": "edge.autoblock.min_score", "from": 100, "to": 80, "actor": "francois", "actorKind": "agent",
         "via": "ai-platform", "reason": "too strict", "detail": {"agent": "Infra Steward", "runId": "run-1", "approvedBy": "francois"},
         "at": "2026-11-13T09:00:00.000Z", "reverts": None},
    ]}, DAY)
    pa.Table.from_pylist(changes, schema=infra_control.CHANGE_SCHEMA)
    assert changes[0]["agent"] == "Infra Steward" and changes[0]["run_id"] == "run-1"


def test_the_change_log_is_read_whole_through_the_cursor(monkeypatch):
    pages = {0: [{"id": n} for n in range(1, 1001)], 1000: [{"id": 1001}, {"id": 1002}]}
    asked = []

    def fake_fetch(path):
        after = int(path.split("after=")[1])
        asked.append(after)
        return {"changes": pages[after]}

    monkeypatch.setattr(infra_control, "_fetch", fake_fetch)
    assert len(infra_control.all_changes()) == 1002
    assert asked == [0, 1000]
