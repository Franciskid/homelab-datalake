"""The alert a failed run sends to the AI platform.

Kept out of definitions.py so it can be tested without importing Dagster and the dbt
manifest, and kept as a pure function so what gets sent is checked by a test rather
than by reading it and hoping.

The platform's `/api/triggers/alertmanager` receiver speaks the subset of
Alertmanager's webhook payload it consumes, so that is the shape produced here: the
datalake is one more alert source next to Prometheus rather than a bespoke
integration the platform has to learn.

**The fingerprint is the whole point.** The platform reconciles by fingerprint: a
repeat of one that is already open updates that incident instead of opening another.
So it is derived from the job and the partition and nothing else -- not the run id,
not the timestamp, not the error text. On 29 Aug 2026 the camera late-arrival
schedule failed every hour for eighteen hours; with a run-id in here that would be
eighteen incidents, and the point of routing this to the platform was to stop having
to read run records by hand.
"""

import datetime
import hashlib
import json
import os
import urllib.request
from typing import Any

# Failing to build the marts stops the serving copy being published, so every reader
# of the lake silently goes stale -- which is exactly the failure that ran for eight
# days in Sep 2026 without anyone noticing. A source partition failing is a gap in one
# day of one dataset, which the next run can repair. They are not the same emergency.
_CRITICAL_JOBS = frozenset({"analytics_hourly"})

# How long the serving copy may go without a publish before readers are told nothing.
# analytics_hourly publishes every hour; three missed hours is not a slow run.
STALE_AFTER_HOURS = 3

# How far back a recovery resolves the dated incidents a critical failure opened. A
# failure that ran longer than this has long since been seen.
RESOLVE_LOOKBACK_DAYS = 14


def _fingerprint(*parts: str) -> str:
    return hashlib.sha1("|".join(("homelab-datalake", *parts)).encode(), usedforsecurity=False).hexdigest()[:16]


def failure_fingerprint(job_name: str, partition: str, day: datetime.date) -> str:
    """One incident per job and partition -- and, for a critical job, per day.

    A critical job used to keep one fingerprint forever. Its incident opened on
    14 Nov 2026, and the next seven days of hourly failures only bumped the count of
    an incident that had already notified once, so the owner heard nothing for a week.
    A dated fingerprint opens a fresh incident, and a fresh notification, every day it
    keeps failing. A source partition stays undated: it is a gap in one partition,
    and it repairs itself on the next run.
    """
    if job_name in _CRITICAL_JOBS:
        return _fingerprint(job_name, partition, day.isoformat())
    return _fingerprint(job_name, partition)


def legacy_failure_fingerprint(job_name: str, partition: str) -> str:
    """The undated fingerprint every job used before 21 Nov 2026, so it can be resolved."""
    return _fingerprint(job_name, partition)


def resolved_payload(fingerprints: list[str], now: datetime.datetime | None = None) -> dict[str, Any]:
    """Resolves these incidents. The platform ignores a fingerprint with nothing open."""
    at = (now or datetime.datetime.now(datetime.UTC)).isoformat()
    return {
        "alerts": [
            {
                "status": "resolved",
                "fingerprint": fingerprint,
                "startsAt": at,
                "labels": {"alertname": "DatalakeRecovered", "service": "homelab-datalake", "severity": "info"},
                "annotations": {"summary": "homelab-datalake recovered"},
            }
            for fingerprint in fingerprints
        ]
    }


def recovery_fingerprints(job_name: str, partition: str, today: datetime.date) -> list[str]:
    """Every fingerprint a run of this job may have left open: the undated one and the
    last RESOLVE_LOOKBACK_DAYS of dated ones."""
    days = [today - datetime.timedelta(days=back) for back in range(RESOLVE_LOOKBACK_DAYS)]
    return [legacy_failure_fingerprint(job_name, partition)] + [
        _fingerprint(job_name, partition, day.isoformat()) for day in days
    ]


def stale_fingerprint(day: datetime.date) -> str:
    return _fingerprint("serving-stale", day.isoformat())


def stale_payload(
    *,
    day: datetime.date,
    last_publish: datetime.datetime | None,
    detail: str,
    now: datetime.datetime | None = None,
) -> dict[str, Any]:
    """The serving copy every reader uses has not been republished.

    This is the alert that was missing from 16 to 21 Nov 2026: a dead run held the only
    analytics slot and every later run sat queued behind it. Nothing *failed*, so the
    failure sensor had nothing to report, while the warehouse stayed frozen for five
    days. Staleness is checked directly, whatever its cause. Dated like a critical
    failure, so a stale copy notifies again every day it stays stale.
    """
    moment = now or datetime.datetime.now(datetime.UTC)
    since = last_publish.isoformat() if last_publish else "never"
    age = (
        f"{(moment - last_publish).total_seconds() / 3600:.0f} hours"
        if last_publish else "no publish on record"
    )
    return {
        "alerts": [
            {
                "status": "firing",
                "fingerprint": stale_fingerprint(day),
                "startsAt": moment.isoformat(),
                "labels": {
                    "alertname": "DatalakeServingStale",
                    "service": "homelab-datalake",
                    "severity": "critical",
                },
                "annotations": {
                    "summary": f"The datalake serving copy has not been published for {age}",
                    "description": f"Last publish: {since}. {detail}",
                },
            }
        ]
    }


def deliver(payload: dict[str, Any], log) -> bool:
    """POST a payload to the platform's alert receiver. Never raises; says if it went.

    Never raising matters: a sensor that raises turns one broken run into two and
    buries the original error.
    """
    url = os.environ.get("PLATFORM_ALERT_URL")
    token = os.environ.get("PLATFORM_ALERT_TOKEN")
    if not url or not token:
        log.error("PLATFORM_ALERT_URL/PLATFORM_ALERT_TOKEN not configured, not notifying: %s", payload)
        return False
    # Refuse anything but http(s). urlopen also speaks file:// and ftp://, and the
    # bearer token would go to whatever answered.
    if not url.startswith(("http://", "https://")):
        log.error("PLATFORM_ALERT_URL must be http(s); refusing to open %r", url)
        return False
    try:
        request = urllib.request.Request(  # noqa: S310 - scheme checked above
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        )
        urllib.request.urlopen(request, timeout=10).close()  # noqa: S310
        return True
    except Exception:
        log.exception("alert could not be delivered")
        return False


def alert_payload(
    *,
    job_name: str,
    partition: str,
    run_id: str,
    detail: str,
    now: datetime.datetime | None = None,
) -> dict[str, Any]:
    """One firing alert describing a failed run.

    Every label value is a string because the receiver validates them as
    `Record<string, string>`; a partition that arrives as anything else would be
    rejected at the schema and the alert lost at the moment it matters most.
    """
    moment = now or datetime.datetime.now(datetime.UTC)
    at = moment.isoformat()
    fingerprint = failure_fingerprint(job_name, partition, moment.date())

    severity = "critical" if job_name in _CRITICAL_JOBS else "warning"

    return {
        "alerts": [
            {
                "status": "firing",
                "fingerprint": fingerprint,
                "startsAt": at,
                "labels": {
                    "alertname": "DatalakeRunFailed",
                    "service": "homelab-datalake",
                    "job": job_name,
                    "partition": partition,
                    "severity": severity,
                },
                "annotations": {
                    # The summary is what a person sees first, so it carries the two
                    # things that decide whether this needs attention now: which job,
                    # and which partition of it.
                    "summary": f"{job_name} failed (partition {partition})",
                    "description": f"run_id={run_id} -- {detail}",
                },
            }
        ]
    }
