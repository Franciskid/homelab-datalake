"""Dagster code location.

Two things worth noticing here, because they are the design and not boilerplate.

The schedule builds *yesterday* at 03:00, after logrotate has moved and after the
nightly rsync from secondary has landed. Building today's partition on a
schedule would produce a partial day that a later run would have to correct, which
is exactly the ambiguity daily partitions exist to remove.

The failure sensor reuses the notification channel homelab_camera already has,
rather than inventing one. Alerting on failure is on PLAN.md's checklist; a second
alerting stack to maintain is not.
"""

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MultiPartitionKey,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    load_assets_from_modules,
    run_failure_sensor,
    schedule,
)

from homelab_datalake.assets import npm_access, warehouse
from homelab_datalake.assets.npm_access import HOSTS, npm_partitions
from homelab_datalake.resources import LakeStorage, NpmLogSources

all_assets = load_assets_from_modules([npm_access, warehouse])

npm_daily_job = define_asset_job(
    name="npm_daily",
    selection=AssetSelection.groups("npm"),
    partitions_def=npm_partitions,
    description="Land one day of NPM logs for one host, then rebuild the warehouse.",
)

# The raw asset alone, for backfills that should not rebuild the warehouse per day.
npm_backfill_job = define_asset_job(
    name="npm_backfill",
    selection=AssetSelection.assets(npm_access.npm_access_raw),
    partitions_def=npm_partitions,
    description=(
        "Raw landing only. Used for backfills: rebuilding the warehouse once at the "
        "end is far cheaper than once per day, and the result is identical."
    ),
)


@schedule(
    job=npm_backfill_job,
    cron_schedule="0 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's partition for every host, at 03:00 UTC.",
)
def npm_daily_schedule(context: ScheduleEvaluationContext):
    """One run per host, so a failure on one machine does not block the other."""
    day = (context.scheduled_execution_time.date() - __import__("datetime").timedelta(days=1))
    for host in HOSTS:
        key = MultiPartitionKey({"date": day.isoformat(), "host": host})
        yield RunRequest(run_key=f"npm-{host}-{day.isoformat()}", partition_key=key)


@run_failure_sensor(
    description="Posts pipeline failures to the existing homelab notification channel.",
    default_status=DefaultSensorStatus.STOPPED,
)
def notify_on_failure(context):
    """Deliberately a no-op until the webhook is configured.

    Logging loudly and doing nothing is better than a sensor that raises on every
    failure -- that turns one broken run into two, and buries the original error.
    """
    webhook = os.environ.get("FAILURE_WEBHOOK_URL")
    message = f"homelab-datalake run failed: {context.failure_event.message}"
    if not webhook:
        context.log.error("%s (no FAILURE_WEBHOOK_URL configured, not notifying)", message)
        return
    # Refuse anything but http(s). urlopen also speaks file:// and ftp://, so an
    # operator typo -- or an env var set from somewhere less trusted -- could turn a
    # notification into a local file read.
    if not webhook.startswith(("http://", "https://")):
        context.log.error("FAILURE_WEBHOOK_URL must be http(s); refusing to open %r", webhook)
        return

    try:
        import json
        import urllib.request

        request = urllib.request.Request(  # noqa: S310 - scheme checked above
            webhook,
            data=json.dumps({"text": message}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(request, timeout=10).close()  # noqa: S310
    except Exception:
        # original run failure by raising a second one on top of it.
        context.log.exception("failure notification could not be delivered")


defs = Definitions(
    assets=all_assets,
    jobs=[npm_daily_job, npm_backfill_job],
    schedules=[npm_daily_schedule],
    sensors=[notify_on_failure],
    resources={
        "lake": LakeStorage.from_env(),
        "npm_logs": NpmLogSources.from_env(),
    },
)
