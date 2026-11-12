"""Time-bounded managed egress inventory, kept in private deployment configuration.

An address match identifies a network, not a process. SQL combines it with a
bounded service signature and labels that result inferred, never authenticated.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
from datetime import UTC, datetime

ENV_NAME = "DATALAKE_MANAGED_ORIGINS_JSON"
_FIELDS = {"address", "origin_edge", "valid_from", "valid_until", "evidence"}


def _instant(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{ENV_NAME}: {field} must be an ISO timestamp with a timezone")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{ENV_NAME}: {field} must include a timezone")
    return parsed.astimezone(UTC)


def validated_origins(raw: str | None = None) -> list[dict]:
    rows = json.loads(os.environ.get(ENV_NAME, "[]") if raw is None else raw)
    if not isinstance(rows, list) or len(rows) > 200:
        raise ValueError(f"{ENV_NAME} must be an array of at most 200 address bindings")
    intervals: dict[str, list[tuple[datetime, datetime]]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != _FIELDS:
            raise ValueError(f"{ENV_NAME}: each binding must have exactly {sorted(_FIELDS)}")
        address = row["address"]
        if not isinstance(address, str) or str(ipaddress.ip_address(address)) != address:
            raise ValueError(f"{ENV_NAME}: address must be a canonical IPv4 or IPv6 address")
        if not isinstance(row["origin_edge"], str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", row["origin_edge"]):
            raise ValueError(f"{ENV_NAME}: origin_edge must be a short inventory identifier")
        if not isinstance(row["evidence"], str) or not 1 <= len(row["evidence"]) <= 300:
            raise ValueError(f"{ENV_NAME}: evidence must explain why this address binding is known")
        start = _instant(row["valid_from"], "valid_from")
        end = _instant(row["valid_until"], "valid_until") if row["valid_until"] is not None else datetime.max.replace(tzinfo=UTC)
        if end <= start:
            raise ValueError(f"{ENV_NAME}: valid_until must be after valid_from")
        prior = intervals.setdefault(address, [])
        if any(start < old_end and old_start < end for old_start, old_end in prior):
            raise ValueError(f"{ENV_NAME}: overlapping address bindings would duplicate request rows")
        prior.append((start, end))
    return rows


def inventory_fingerprint_input() -> str:
    """A changed binding must rebuild historical classifications, not just new rows."""
    return json.dumps(validated_origins(), sort_keys=True, separators=(",", ":"))
