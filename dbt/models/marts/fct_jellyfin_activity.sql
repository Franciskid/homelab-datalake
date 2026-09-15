-- One row per server activity log entry: sign-ins, playback starts and stops,
-- library and account changes -- whatever Jellyfin itself decided to log.
--
-- user_name is joined from dim_jellyfin_user rather than carried on the raw row the
-- way playback's is: the activity log only ever gives a raw UserId, and the account
-- snapshot already has to exist for jellyfin_users_raw, so a second name lookup at
-- ingest would be a second thing to keep working for a mapping this join already
-- provides.

select
    activity.activity_id,
    activity.partition_date,
    activity.extracted_at,
    activity.happened_at,
    activity.happened_hour,
    extract('hour' from activity.happened_at)            as happened_hour_of_day,
    dayname(activity.happened_at)                         as happened_weekday,
    (dayofweek(activity.happened_at) in (0, 6))           as is_weekend,
    activity.activity_type,
    activity.name,
    activity.overview,
    activity.short_overview,
    activity.user_id,
    users.user_name,
    activity.item_id,
    activity.severity,
    activity.is_error,
    activity.is_warning,
    activity.unmapped_json
from {{ ref('stg_jellyfin_activity') }} as activity
left join {{ ref('dim_jellyfin_user') }} as users
    on activity.user_id = users.user_id
