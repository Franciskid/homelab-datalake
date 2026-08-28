-- Exactly one version of each event survives into the fact table.
--
-- The raw layer holds several rows per event on purpose: each re-read of a partition
-- writes the event as it stood then, and analysis arriving later changes it. If the
-- dedup in int_camera_events ever breaks, every count of confirmed events inflates
-- silently rather than erroring, which is the worst way for this to fail.

select event_id, count(*) as versions
from {{ ref('fct_camera_event') }}
group by event_id
having count(*) > 1
