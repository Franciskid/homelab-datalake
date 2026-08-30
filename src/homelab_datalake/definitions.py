"""Dagster code location.

Two things worth noticing here, because they are the design and not boilerplate.

The schedules build *yesterday* at 03:00, after logrotate has moved. Building
today's partition on a schedule would produce a partial day that a later run would
have to correct, which is exactly the ambiguity daily partitions exist to remove.

The sources land between 03:00 and 03:45, and `analytics_daily` at 04:15 turns them
into marts and publishes the serving copy. That last job is what makes the numbers
the MCP layer serves the same age as the data underneath -- before it existed the
sources refreshed nightly and the marts refreshed whenever somebody remembered to
run dbt, which is a way of being wrong that never announces itself.

The failure sensor reuses the notification channel homelab_camera already has,
rather than inventing one. Alerting on failure is on PLAN.md's checklist; a second
alerting stack to maintain is not.
"""

import datetime
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
from dagster_dbt import DbtCliResource

from homelab_datalake.assets import (
    backup,
    camera,
    dbt_marts,
    jellyfin,
    litellm_spend,
    npm_access,
    publish,
    tmdb,
    warehouse,
)
from homelab_datalake.assets.npm_access import HOSTS, npm_partitions
from homelab_datalake.resources import LakeStorage, NpmLogSources

all_assets = load_assets_from_modules(
    [npm_access, litellm_spend, jellyfin, camera, tmdb, dbt_marts, warehouse, publish,
     backup]
)

npm_daily_job = define_asset_job(
    name="npm_daily",
    selection=AssetSelection.groups("npm"),
    partitions_def=npm_partitions,
    description="Land one day of NPM logs for one host.",
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


litellm_daily_job = define_asset_job(
    name="litellm_daily",
    selection=AssetSelection.groups("litellm"),
    partitions_def=litellm_spend.spend_partitions,
    description="Land one day of LiteLLM spend.",
)


@schedule(
    job=litellm_daily_job,
    cron_schedule="15 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's LLM spend, at 03:15 UTC -- after the log partitions.",
)
def litellm_daily_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    yield RunRequest(run_key=f"litellm-{day.isoformat()}", partition_key=day.isoformat())


jellyfin_daily_job = define_asset_job(
    name="jellyfin_daily",
    selection=AssetSelection.groups("jellyfin"),
    partitions_def=jellyfin.library_partitions,
    description="Snapshot the media library.",
)


@schedule(
    job=jellyfin_daily_job,
    cron_schedule="30 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Today's library at 03:30 UTC. Today, not yesterday: a snapshot describes the "
        "library now, and dating it yesterday would invent a past that never existed."
    ),
)
def jellyfin_daily_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date()
    yield RunRequest(run_key=f"jellyfin-{day.isoformat()}", partition_key=day.isoformat())


camera_daily_job = define_asset_job(
    name="camera_daily",
    selection=AssetSelection.groups("camera"),
    partitions_def=camera.camera_partitions,
    description="Land one day of camera events, clips and logs.",
)


@schedule(
    job=camera_daily_job,
    cron_schedule="45 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's camera events at 03:45 UTC.",
)
def camera_daily_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    yield RunRequest(run_key=f"camera-{day.isoformat()}", partition_key=day.isoformat())


@schedule(
    job=camera_daily_job,
    cron_schedule="20 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Re-reads the last two days hourly, because vision analysis lands hours after "
        "the event. Without this the summaries and confidences for yesterday evening "
        "would not appear until the next nightly run. Idempotent by construction: the "
        "mart keeps the newest version of each event."
    ),
)
def camera_late_arrival_schedule(context: ScheduleEvaluationContext):
    today = context.scheduled_execution_time.date()
    for back in (0, 1):
        day = today - datetime.timedelta(days=back)
        yield RunRequest(
            run_key=f"camera-late-{day.isoformat()}-{context.scheduled_execution_time:%Y%m%d%H}",
            partition_key=day.isoformat(),
        )


@schedule(
    job=npm_backfill_job,
    cron_schedule="0 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's partition for every host, at 03:00 UTC.",
)
def npm_daily_schedule(context: ScheduleEvaluationContext):
    """One run per host, so a failure on one machine does not block the other."""
    day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    for host in HOSTS:
        key = MultiPartitionKey({"date": day.isoformat(), "host": host})
        yield RunRequest(run_key=f"npm-{host}-{day.isoformat()}", partition_key=key)


# Everything downstream of the raw partitions: load the lake into DuckDB, build and
# test the dbt models, then publish the serving copy. One job rather than three, so
# there is no window in which the marts are newer than the warehouse or the serving
# copy is newer than the marts.
analytics_job = define_asset_job(
    name="analytics_daily",
    selection=AssetSelection.groups("warehouse") | AssetSelection.assets(dbt_marts.dbt_marts),
    description=(
        "Rebuild the warehouse from the lake, build and test the dbt models, publish "
        "the serving copy the MCP layer reads."
    ),
)


@schedule(
    job=analytics_job,
    cron_schedule="15 4 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "04:15 UTC, after the last source lands at 03:45. Half an hour of slack rather "
        "than the couple of minutes the runs actually take: a retry on a slow source "
        "should still finish before the marts are built, and a mart built from a "
        "partition that had not landed yet is silently short by a day."
    ),
)
def analytics_daily_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"analytics-{context.scheduled_execution_time:%Y-%m-%d}")


@run_failure_sensor(
    description="Posts pipeline failures to the existing homelab notification channel.",
    # RUNNING, not STOPPED.
    #
    # This was STOPPED by default and nobody ever started it, so it was dead twice
    # over: no webhook *and* not running. The cost was measured. On 29 Aug 2026 the
    # LiteLLM ingest failed because its Postgres network had gone, and the camera
    # late-arrival schedule failed every hour for eighteen hours. Both were found by
    # reading run records by hand, two days later.
    #
    # A sensor that only writes to a log is worth little; a sensor that is switched
    # off is worth nothing. Starting by default costs one tick per failed run, and the
    # tick itself is the record -- it is visible in the Dagster UI beside the run,
    # which is where somebody looking at a red asset already is.
    default_status=DefaultSensorStatus.RUNNING,
)
def notify_on_failure(context):
    """Reports a failed run, and delivers it if a webhook is configured.

    Logging loudly and doing nothing is better than a sensor that raises on every
    failure -- that turns one broken run into two, and buries the original error.
    """
    run = context.dagster_run
    # The job and the partition, not only the exception text. Diagnosing the two
    # August failures meant going back to the run records to find out *which*
    # partition of which job had died; the message should carry that already.
    partition = run.tags.get("dagster/partition") or "-"
    message = (
        f"homelab-datalake run failed: job={run.job_name} partition={partition} "
        f"run_id={run.run_id} -- {context.failure_event.message}"
    )

    webhook = os.environ.get("FAILURE_WEBHOOK_URL")
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
    jobs=[npm_daily_job, npm_backfill_job, litellm_daily_job, jellyfin_daily_job,
          camera_daily_job, analytics_job],
    schedules=[npm_daily_schedule, litellm_daily_schedule, jellyfin_daily_schedule,
               camera_daily_schedule, camera_late_arrival_schedule,
               analytics_daily_schedule],
    sensors=[notify_on_failure],
    resources={
        "lake": LakeStorage.from_env(),
        "npm_logs": NpmLogSources.from_env(),
        "dbt": DbtCliResource(project_dir=dbt_marts.dbt_project),
    },
)
