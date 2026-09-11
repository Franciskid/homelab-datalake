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
from typing import Any

# Failing to build the marts stops the serving copy being published, so every reader
# of the lake silently goes stale -- which is exactly the failure that ran for eight
# days in Sep 2026 without anyone noticing. A source partition failing is a gap in one
# day of one dataset, which the next run can repair. They are not the same emergency.
_CRITICAL_JOBS = frozenset({"analytics_daily"})


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
    at = (now or datetime.datetime.now(datetime.UTC)).isoformat()
    fingerprint = hashlib.sha1(
        f"homelab-datalake|{job_name}|{partition}".encode(),
        usedforsecurity=False,
    ).hexdigest()[:16]

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
