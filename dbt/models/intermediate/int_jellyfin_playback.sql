-- One row per playback session: the latest version of it.
--
-- PlaybackActivity carries no id at all, so (started_at, user_id, item_id) is the
-- closest thing to one -- and jellyfin_playback_raw already merges on exactly that
-- key rather than replacing a partition, because a long session can still be
-- invisible to the plugin's own query when its own hour is first read. In normal
-- operation the raw layer therefore already holds one row per key.
--
-- This stays as a guard rather than a mechanism, the same shape as int_camera_events
-- and for the same reason: the cost is one window function, and the failure it
-- prevents is silent -- a partition landing as more than one object would inflate
-- every watch-time measure with no error anywhere. The accompanying test asserts the
-- fact table has one row per session whether or not this model had to do anything.

with versions as (

    select
        *,
        row_number() over (
            partition by started_at, user_id, item_id
            order by extracted_at desc
        ) as recency
    from {{ ref('stg_jellyfin_playback') }}

)

select * exclude (recency)
from versions
where recency = 1
