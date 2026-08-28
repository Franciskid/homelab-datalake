-- One row per event: the latest version of it.
--
-- This model is the whole reason the camera source is interesting. Vision analysis is
-- asynchronous, so an event is first written as `candidate` with no confidence and no
-- summary, and is filled in minutes or hours later. A partition re-read tomorrow
-- therefore returns *different* rows than it did today -- correctly.
--
-- So "re-running a partition replaces it exactly" is not quite the property this source
-- has. The precise version is: re-running replaces a partition with the current truth
-- for that date, and the marts always reflect the latest version of each event. That
-- holds because every raw row carries `_extracted_at` and this model keeps the newest.
--
-- Ordering by extracted_at alone is not enough: a backfill can write two versions
-- inside the same second. `lifecycle <> 'candidate'` breaks the tie towards the
-- analysed version, because an analysed event is never superseded by an unanalysed one.

with versions as (

    select
        *,
        row_number() over (
            partition by event_id
            order by extracted_at desc, (lifecycle <> 'candidate') desc
        ) as recency
    from {{ ref('stg_camera_events') }}

)

select * exclude (recency)
from versions
where recency = 1
