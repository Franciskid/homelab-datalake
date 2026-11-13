"""fraserv-infra's infra-control: the edge decisions, the global settings and their history.

infra-control owns the NPM blocklist and the installation's global settings since the
cutover (fraserv-infra docs/control/transfer.md). Before it, the AI platform owned the
decisions, which landed from its edge_blocks table; infra-control mirrored them with the
same ids during its shadow weeks, so its export is the whole history, old rows included.

Read from its export routes (`GET /v1/export/decisions`, `/v1/export/config`,
`/v1/export/changes`) with the lake's own client token (scopes read and export). Each
export is complete, so a day's partition is a snapshot, replaced whole on every run, and
the newest partition is the current state. Three tables:

- infra_control_edge_decisions: every block and never-block, lifted and expired ones too.
- infra_control_settings: every registry setting, its value, where it comes from.
- infra_control_config_changes: the change log -- who, through what (the Infra tab, an
  approved agent and which run), from what to what, why.
"""

import json
import os
import urllib.request
from datetime import UTC, date, datetime

import pyarrow as pa
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    RetryPolicy,
    asset,
)

from homelab_datalake import layout
from homelab_datalake.resources import LakeStorage

DEFAULT_URL = "http://infra-control:9470"
REQUEST_TIMEOUT_SECONDS = 60
CHECK_LANDED = "every_row_landed"

infra_control_partitions = DailyPartitionsDefinition(start_date="2026-11-13", timezone="UTC", end_offset=1)

DECISIONS_PREFIX = layout.prefix("infra_control_edge_decisions")
SETTINGS_PREFIX = layout.prefix("infra_control_settings")
CHANGES_PREFIX = layout.prefix("infra_control_config_changes")

_TS = pa.timestamp("ms", tz="UTC")

DECISION_SCHEMA = pa.schema([
    ("dt", pa.date32()), ("id", pa.string()), ("cidr", pa.string()), ("action", pa.string()),
    ("source", pa.string()), ("reason", pa.string()), ("score", pa.int64()), ("created_by", pa.string()),
    ("created_at", _TS), ("expires_at", _TS), ("revoked_at", _TS), ("revoked_by", pa.string()), ("via", pa.string()),
])
SETTING_SCHEMA = pa.schema([
    ("dt", pa.date32()), ("key", pa.string()), ("type", pa.string()), ("value_json", pa.string()),
    ("default_json", pa.string()), ("tier", pa.string()), ("managed_by", pa.string()), ("origin", pa.string()),
    ("ai_writable", pa.bool_()), ("updated_by", pa.string()), ("updated_at", _TS),
])
CHANGE_SCHEMA = pa.schema([
    ("dt", pa.date32()), ("change_id", pa.int64()), ("key", pa.string()), ("from_json", pa.string()),
    ("to_json", pa.string()), ("actor", pa.string()), ("actor_kind", pa.string()), ("via", pa.string()),
    ("reason", pa.string()), ("agent", pa.string()), ("run_id", pa.string()), ("changed_at", _TS),
    ("reverts", pa.int64()),
])


def _time(value) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _fetch(path: str) -> dict:
    base = os.environ.get("INFRA_CONTROL_URL", DEFAULT_URL).rstrip("/")
    token = os.environ.get("INFRA_CONTROL_TOKEN", "")
    if not token:
        raise RuntimeError("INFRA_CONTROL_TOKEN is not set: the lake has no client at infra-control")
    if not base.startswith(("http://", "https://")):
        raise RuntimeError(f"INFRA_CONTROL_URL must be http(s): {base!r}")
    request = urllib.request.Request(  # noqa: S310 -- scheme checked just above
        f"{base}{path}", headers={"authorization": f"Bearer {token}", "accept": "application/json"})
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310
        return json.load(response)


def decision_rows(export: dict, day: date) -> list[dict]:
    return [{
        "dt": day, "id": d["id"], "cidr": d["cidr"], "action": d["action"], "source": d["source"],
        "reason": d.get("reason"), "score": d.get("score"), "created_by": d.get("createdBy"),
        "created_at": _time(d.get("createdAt")), "expires_at": _time(d.get("expiresAt")),
        "revoked_at": _time(d.get("revokedAt")), "revoked_by": d.get("revokedBy"), "via": d.get("via") or None,
    } for d in export.get("decisions", [])]


def setting_rows(export: dict, day: date) -> list[dict]:
    return [{
        "dt": day, "key": s["key"], "type": s["type"], "value_json": json.dumps(s.get("value")),
        "default_json": json.dumps(s.get("default")), "tier": s.get("tier"), "managed_by": s.get("managedBy"),
        "origin": s.get("origin"), "ai_writable": bool((s.get("ai") or {}).get("writable")),
        "updated_by": s.get("updatedBy"), "updated_at": _time(s.get("updatedAt")),
    } for s in export.get("settings", [])]


def change_rows(export: dict, day: date) -> list[dict]:
    return [{
        "dt": day, "change_id": c["id"], "key": c["key"], "from_json": json.dumps(c.get("from")),
        "to_json": json.dumps(c.get("to")), "actor": c.get("actor"), "actor_kind": c.get("actorKind"),
        "via": c.get("via"), "reason": c.get("reason"), "agent": (c.get("detail") or {}).get("agent"),
        "run_id": (c.get("detail") or {}).get("runId"), "changed_at": _time(c.get("at")), "reverts": c.get("reverts"),
    } for c in export.get("changes", [])]


def all_changes() -> list[dict]:
    """The whole change log, oldest first, following infra-control's cursor."""
    out: list[dict] = []
    after = 0
    while True:
        page = _fetch(f"/v1/export/changes?after={after}").get("changes", [])
        out.extend(page)
        if len(page) < 1000:
            return out
        after = page[-1]["id"]


@asset(
    partitions_def=infra_control_partitions,
    group_name="infra_control",
    kinds={"python"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
    check_specs=[AssetCheckSpec(
        name=CHECK_LANDED, asset="infra_control_raw",
        description="Every decision, setting and change the export returned is in the day's objects.",
    )],
    description=(
        "A day's snapshot of fraserv-infra's infra-control: every edge decision (lifted and "
        "expired included), every global setting with its value, and the settings change log."
    ),
)
def infra_control_raw(context: AssetExecutionContext, lake: LakeStorage) -> MaterializeResult:
    day = date.fromisoformat(context.partition_key)
    decisions = decision_rows(_fetch("/v1/export/decisions"), day)
    settings = setting_rows(_fetch("/v1/export/config"), day)
    changes = change_rows({"changes": all_changes()}, day)
    landed = {}
    for prefix, rows, schema in ((DECISIONS_PREFIX, decisions, DECISION_SCHEMA),
                                 (SETTINGS_PREFIX, settings, SETTING_SCHEMA),
                                 (CHANGES_PREFIX, changes, CHANGE_SCHEMA)):
        key = f"{prefix}/dt={day.isoformat()}/data.parquet"
        lake.write_table(key, pa.Table.from_pylist(rows, schema=schema))
        landed[prefix] = lake.count_rows(key) or 0
    expected = {DECISIONS_PREFIX: len(decisions), SETTINGS_PREFIX: len(settings), CHANGES_PREFIX: len(changes)}
    context.log.info("%s decisions=%d settings=%d changes=%d", day, len(decisions), len(settings), len(changes))
    return MaterializeResult(
        metadata={"decisions": len(decisions), "settings": len(settings), "changes": len(changes)},
        check_results=[AssetCheckResult(
            check_name=CHECK_LANDED, passed=landed == expected, severity=AssetCheckSeverity.ERROR,
            metadata={"landed": json.dumps(landed), "expected": json.dumps(expected)},
        )],
    )


ASSETS = [infra_control_raw]
JOBS: list = []
SCHEDULES: list = []
