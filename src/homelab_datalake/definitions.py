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
    ai_platform,
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
from homelab_datalake.parsing.ai_platform_registry import WINDOW_DAYS
from homelab_datalake.resources import LakeStorage, NpmLogSources

all_assets = load_assets_from_modules(
    [npm_access, litellm_spend, jellyfin, camera, tmdb, ai_platform, dbt_marts,
     warehouse, publish, backup]
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


# Playback, the activity log and the account list are excluded and get their own jobs
# below: each has its own partitions definition -- hourly for playback, its own start
# date for the other two -- and everything in one asset job has to share one, the same
# reason camera_zones_raw is split out of camera_daily_job.
# Snapshot tables (agents, applications, schedules, and every other config/entity
# table in the registry) get one asset per day, same shape as jellyfin_daily below:
# small enough to re-read whole, so there is nothing to re-extract later.
ai_platform_snapshot_job = define_asset_job(
    name="ai_platform_snapshot",
    selection=AssetSelection.assets(*ai_platform.snapshot_asset_keys),
    partitions_def=ai_platform.ai_platform_partitions,
    description="Today's snapshot of every config/entity table in the AI platform's database.",
)


@schedule(
    job=ai_platform_snapshot_job,
    cron_schedule="0 5 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Today's snapshot of every AI-platform config/entity table at 05:00 UTC, "
        "after the existing 03:xx sources and the 04:15 analytics build."
    ),
)
def ai_platform_snapshot_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date()
    yield RunRequest(
        run_key=f"ai-platform-snapshot-{day.isoformat()}", partition_key=day.isoformat()
    )


# Event-shaped tables (usage_events, runs, run_steps, messages, and so on) get a
# trailing re-extraction window: every tick re-fires the last WINDOW_DAYS partitions,
# each overwriting its own day idempotently, so a row that changed after it first
# landed -- a run completing, an approval being decided -- is picked up on its next
# re-run rather than frozen at its first-seen values. Same shape as
# camera_late_arrival_schedule below, at a window sized for this platform's own
# lifecycles rather than the camera's -- see ai_platform_registry.WINDOW_DAYS.
ai_platform_window_job = define_asset_job(
    name="ai_platform_window",
    selection=AssetSelection.assets(*ai_platform.window_asset_keys),
    partitions_def=ai_platform.ai_platform_partitions,
    description="Re-extract one day of every event-shaped AI-platform table.",
)


@schedule(
    job=ai_platform_window_job,
    cron_schedule="10 5 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        f"Re-extracts the trailing {WINDOW_DAYS} days of every event-shaped "
        "AI-platform table at 05:10 UTC, so a row that changes within that window "
        "after it first lands -- a run completing, an approval being decided -- is "
        "corrected rather than left at its first-seen values. Idempotent: each "
        "day's re-run overwrites only that day's own partition."
    ),
)
def ai_platform_window_schedule(context: ScheduleEvaluationContext):
    today = context.scheduled_execution_time.date()
    for back in range(WINDOW_DAYS):
        day = today - datetime.timedelta(days=back)
        yield RunRequest(
            run_key=f"ai-platform-window-{day.isoformat()}-{context.scheduled_execution_time:%Y%m%d}",
            partition_key=day.isoformat(),
        )


ai_platform_coverage_job = define_asset_job(
    # Named distinctly from the asset it selects: `define_asset_job` derives an op
    # inside the implicit `__ASSET_JOB` with the asset's own name, and giving this
    # job the identical name collided with it -- "Conflicting definitions... Op/Graph
    # definition names must be unique within a repository."
    name="ai_platform_coverage",
    selection=AssetSelection.assets(ai_platform.ai_platform_registry_coverage),
    description="Check the registry against ai_platform's live schema.",
)


@schedule(
    job=ai_platform_coverage_job,
    cron_schedule="20 5 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Compares ai_platform_registry.REGISTRY against information_schema.tables at "
        "05:20 UTC, so a table the platform adds later is caught rather than simply "
        "never arriving."
    ),
)
def ai_platform_coverage_schedule(context: ScheduleEvaluationContext):
    yield RunRequest(run_key=f"ai-platform-coverage-{context.scheduled_execution_time:%Y-%m-%d}")


jellyfin_daily_job = define_asset_job(
    name="jellyfin_daily",
    selection=(
        AssetSelection.groups("jellyfin")
        - AssetSelection.assets(
            jellyfin.jellyfin_playback_raw, jellyfin.jellyfin_activity_raw,
            jellyfin.jellyfin_users_raw, jellyfin.jellyfin_library_items_raw,
            jellyfin.jellyfin_activity_history_raw,
            jellyfin.jellyfin_playback_history_raw,
        )
    ),
    partitions_def=jellyfin.library_partitions,
    description="Snapshot the movie library.",
)

jellyfin_library_items_job = define_asset_job(
    name="jellyfin_library_items",
    selection=AssetSelection.assets(jellyfin.jellyfin_library_items_raw),
    partitions_def=jellyfin.library_items_partitions,
    description="Snapshot today's series, seasons, episodes, box sets and other videos.",
)

# Unpartitioned, and deliberately without a schedule: this is a one-time load, run
# once by hand after ACTIVITY_START moved back to the log's real first day. See
# ACTIVITY_START's own comment in assets/jellyfin.py for why the history arrives
# this way instead of a [count withheld]-partition backfill of jellyfin_activity_job below.
jellyfin_activity_history_job = define_asset_job(
    name="jellyfin_activity_history",
    selection=AssetSelection.assets(jellyfin.jellyfin_activity_history_raw),
    description="One-time, one-pass load of the full activity log history.",
)

# Unpartitioned and deliberately without a schedule, for the same reason as the
# activity history job above: a one-time load, run once by hand. See PLAYBACK_START's
# own comment in assets/jellyfin.py for why the plugin's back catalogue arrives this
# way rather than through [count withheld] partitions of jellyfin_playback_job below -- or
# through one range run over them, which would write every one of those sessions into
# a single hour's object.
jellyfin_playback_history_job = define_asset_job(
    name="jellyfin_playback_history",
    selection=AssetSelection.assets(jellyfin.jellyfin_playback_history_raw),
    description="One-time, one-pass load of the full Playback Reporting history.",
)

jellyfin_playback_job = define_asset_job(
    name="jellyfin_playback",
    selection=AssetSelection.assets(jellyfin.jellyfin_playback_raw),
    partitions_def=jellyfin.playback_partitions,
    description="Land one hour of Playback Reporting plugin rows.",
)

jellyfin_activity_job = define_asset_job(
    name="jellyfin_activity",
    selection=AssetSelection.assets(jellyfin.jellyfin_activity_raw),
    partitions_def=jellyfin.activity_partitions,
    description="Land one day of the server activity log.",
)

jellyfin_users_job = define_asset_job(
    name="jellyfin_users",
    selection=AssetSelection.assets(jellyfin.jellyfin_users_raw),
    partitions_def=jellyfin.users_partitions,
    description="Snapshot today's account list.",
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


@schedule(
    job=jellyfin_library_items_job,
    cron_schedule="32 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Today's series, seasons, episodes, box sets and other videos, at 03:32 UTC, "
        "two minutes after the movie snapshot. Today's, not yesterday's, for the same "
        "reason the movie snapshot is: this describes the library now. Measured at "
        "[duration withheld] for the roughly [count withheld] non-movie items on this server -- see "
        "parsing/jellyfin.py for why (`People` is not requested) and for what the "
        "same read cost with it included ([duration withheld])."
    ),
)
def jellyfin_library_items_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date()
    yield RunRequest(run_key=f"jellyfin-library-items-{day.isoformat()}", partition_key=day.isoformat())


@schedule(
    job=jellyfin_playback_job,
    cron_schedule="5 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Every hour at five past, re-materialising the current hour plus the three "
        "before it -- one combined schedule rather than camera's two, because "
        "playback's own partition grain is already hourly, so the same tick that lands "
        "a fresh hour can also be the late-arrival re-read. Four hours because the "
        "plugin does not expose whether a session is still open: the longest measured "
        "on this server is Example Movie at [duration withheld] (1h52m), and this doubles that "
        "with room to spare. Idempotent by construction -- jellyfin_playback_raw "
        "merges into a partition rather than replacing it."
    ),
)
def jellyfin_playback_hourly_schedule(context: ScheduleEvaluationContext):
    current_hour = context.scheduled_execution_time.replace(
        minute=0, second=0, microsecond=0
    )
    for back in range(4):
        hour = current_hour - datetime.timedelta(hours=back)
        yield RunRequest(
            run_key=f"jellyfin-playback-{hour:%Y-%m-%d-%H}-{context.scheduled_execution_time:%Y%m%d%H%M}",
            partition_key=hour.strftime("%Y-%m-%d-%H:%M"),
        )


@schedule(
    job=jellyfin_activity_job,
    cron_schedule="35 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Yesterday's activity log at 03:35 UTC -- a log day has to be over first.",
)
def jellyfin_activity_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
    yield RunRequest(run_key=f"jellyfin-activity-{day.isoformat()}", partition_key=day.isoformat())


@schedule(
    job=jellyfin_users_job,
    cron_schedule="38 3 * * *",
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Today's account list at 03:38 UTC. Today's, not yesterday's: an account list "
        "has no history at the API, so the only date this snapshot can honestly carry "
        "is the one it was taken on."
    ),
)
def jellyfin_users_schedule(context: ScheduleEvaluationContext):
    day = context.scheduled_execution_time.date()
    yield RunRequest(run_key=f"jellyfin-users-{day.isoformat()}", partition_key=day.isoformat())


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
          ai_platform_snapshot_job, ai_platform_window_job, ai_platform_coverage_job,
          jellyfin_daily_job, jellyfin_library_items_job, jellyfin_playback_job,
          jellyfin_playback_history_job, jellyfin_activity_job,
          jellyfin_activity_history_job, jellyfin_users_job,
          camera_daily_job, camera_zones_job, analytics_job],
    schedules=[npm_daily_schedule, mcp_api_daily_schedule, litellm_daily_schedule,
               ai_platform_snapshot_schedule, ai_platform_window_schedule,
               ai_platform_coverage_schedule, jellyfin_daily_schedule,
               jellyfin_library_items_schedule, jellyfin_playback_hourly_schedule,
               jellyfin_activity_schedule, jellyfin_users_schedule, camera_daily_schedule,
               camera_late_arrival_schedule, camera_zones_schedule, analytics_daily_schedule],
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
