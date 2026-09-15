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

from homelab_datalake.alerting import alert_payload
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

# mcp_api_access_raw is excluded and gets its own job below, the same way
# camera_zones_raw is excluded from camera_daily_job: it shares the "npm" group
# because it lands in the same raw.npm_access table, but it has its own partitions
# definition (its own start date -- see npm_access.MCP_API_LOG_START), and a job
# cannot mix two partitions definitions under one `partitions_def=`.
npm_daily_job = define_asset_job(
    name="npm_daily",
    selection=AssetSelection.groups("npm") - AssetSelection.keys("mcp_api_access_raw"),
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

mcp_api_daily_job = define_asset_job(
    name="mcp_api_daily",
    selection=AssetSelection.assets(npm_access.mcp_api_access_raw),
    partitions_def=npm_access.mcp_api_partitions,
    description="Land one day of mcp.ai.example.test's own access log.",
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


# Zones are excluded and get their own job below. Everything in a partitioned job has
# to share one partitions definition, and a zone snapshot cannot honestly carry the
# event partitions' start date -- see `camera.ZONE_START`.
camera_daily_job = define_asset_job(
    name="camera_daily",
    selection=AssetSelection.groups("camera") - AssetSelection.keys("camera_zones_raw"),
    partitions_def=camera.camera_partitions,
    description=(
        "Land one day of camera events, clips, logs, coverage spans and annotations."
    ),
)

camera_zones_job = define_asset_job(
    name="camera_zones",
    selection=AssetSelection.keys("camera_zones_raw"),
    partitions_def=camera.zone_partitions,
    description="Snapshot today's attention-zone configuration.",
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
    job=camera_zones_job,
    cron_schedule="40 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Today's zone configuration at 03:40 UTC, five minutes before the events that "
        "will be read against it. Today's, not yesterday's: the camera has no history "
        "for a zone, so the only date this snapshot can honestly carry is the one it "
        "was taken on."
    ),
)
def camera_zones_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date()
    yield RunRequest(run_key=f"camera-zones-{day.isoformat()}", partition_key=day.isoformat())


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


@schedule(
    job=mcp_api_daily_job,
    cron_schedule="10 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's mcp.ai access log at 03:10 UTC, ahead of the NPM partitions.",
)
def mcp_api_daily_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    yield RunRequest(run_key=f"mcp-api-{day.isoformat()}", partition_key=day.isoformat())


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
    description="Opens an incident on the AI platform when a run fails.",
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
    #
    # It was still not enough. The sensor ran, and it had no webhook to deliver to, so
    # it logged -- and analytics_daily then failed every night from 4 to 11 Sep 2026
    # while the serving copy every reader depends on stayed frozen at 2 Sep. Eight
    # days, into a log nobody reads. Failures now go to the AI platform, which is
    # where incidents already live and where something actually looks at them.
    default_status=DefaultSensorStatus.RUNNING,
)
def notify_on_failure(context):
    """Reports a failed run, and opens an incident if the platform is configured.

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

    url = os.environ.get("PLATFORM_ALERT_URL")
    token = os.environ.get("PLATFORM_ALERT_TOKEN")
    if not url or not token:
        context.log.error(
            "%s (PLATFORM_ALERT_URL/PLATFORM_ALERT_TOKEN not configured, not notifying)",
            message,
        )
        return
    # Refuse anything but http(s). urlopen also speaks file:// and ftp://, so an
    # operator typo -- or an env var set from somewhere less trusted -- could turn a
    # notification into a local file read. The bearer token makes this stricter than
    # it looks: the wrong scheme here would hand that secret to whatever answered.
    if not url.startswith(("http://", "https://")):
        context.log.error("PLATFORM_ALERT_URL must be http(s); refusing to open %r", url)
        return

    try:
        import json
        import urllib.request

        request = urllib.request.Request(  # noqa: S310 - scheme checked above
            url,
            data=json.dumps(
                alert_payload(
                    job_name=run.job_name,
                    partition=partition,
                    run_id=run.run_id,
                    detail=context.failure_event.message,
                )
            ).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        urllib.request.urlopen(request, timeout=10).close()  # noqa: S310
        context.log.info("opened a platform incident for %s/%s", run.job_name, partition)
    except Exception:
        # Never raise: that would compound the original run failure by raising a
        # second one on top of it, and bury the error that started this.
        context.log.exception("failure notification could not be delivered")


defs = Definitions(
    assets=all_assets,
    jobs=[npm_daily_job, npm_backfill_job, mcp_api_daily_job, litellm_daily_job,
          jellyfin_daily_job, camera_daily_job, camera_zones_job, analytics_job],
    schedules=[npm_daily_schedule, mcp_api_daily_schedule, litellm_daily_schedule,
               jellyfin_daily_schedule, camera_daily_schedule, camera_late_arrival_schedule,
               camera_zones_schedule, analytics_daily_schedule],
    sensors=[notify_on_failure],
    resources={
        "lake": LakeStorage.from_env(),
        "npm_logs": NpmLogSources.from_env(),
        "dbt": DbtCliResource(project_dir=dbt_marts.dbt_project),
    },
)


# --------------------------------------------------------------------------------
# Sources added 2026-09-15: the Prometheus stream, the status page, Radarr, Sonarr and
# Jellyseerr, primary's host logs, Keycloak sign-ins. See docs/METRICS_STREAMING_PLAN.md.
#
# Merged in as a second Definitions rather than written into the lists above, so the
# sources other branches add there do not collide with these lines.
# --------------------------------------------------------------------------------

from homelab_datalake.assets import (  # noqa: E402
    host_logs,
    keycloak,
    media_pipeline,
    prometheus,
    status_page,
)

_added_sources = (prometheus, status_page, media_pipeline, host_logs, keycloak)

defs = Definitions.merge(
    defs,
    Definitions(
        assets=[a for module in _added_sources for a in module.ASSETS],
        jobs=[j for module in _added_sources for j in module.JOBS],
        schedules=[s for module in _added_sources for s in module.SCHEDULES],
        sensors=prometheus.SENSORS,
    ),
)
