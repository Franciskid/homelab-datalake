-- One row per event: the latest version of it.
--
-- Vision analysis is asynchronous: an event is first written as `candidate` with no
-- confidence and no summary, and is filled in minutes or hours later. So a partition
-- re-read tomorrow returns *different* rows than it did today -- correctly, and the
-- hourly schedule re-reads the last two days for exactly that reason.
--
-- What actually keeps the marts current is that re-materialising a partition
-- **replaces** it: the asset deletes the prefix and writes one object. So in normal
-- operation the raw layer holds exactly one version of each event, and this model
-- collapses nothing -- measured, re-reading three partitions produced 0 superseded
-- rows. Saying the dedup is what handles late arrival would be claiming work it does
-- not do.
--
-- It stays as a guard rather than a mechanism, because the cost is one window function
-- and the failure it prevents is silent: if a partition ever landed as several objects
-- -- a partial write, a schema migration writing beside the old file -- every count of
-- confirmed events would inflate with no error anywhere. The accompanying test asserts
-- the fact table has one row per event whether or not this model had to do anything.
--
-- Ordering by extracted_at alone would not be enough for that case: two versions can
-- share a second. `lifecycle <> 'candidate'` breaks the tie towards the analysed
-- version, because an analysed event is never superseded by an unanalysed one.

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
