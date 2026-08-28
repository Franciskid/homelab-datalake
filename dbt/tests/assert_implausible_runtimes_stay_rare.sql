-- Titles whose reported runtime cannot be right are a known, small population: two
-- mpeg2video DVD rips claiming [count withheld] and [count withheld] minutes. That is a source metadata
-- problem, kept visible rather than corrected.
--
-- This test does not object to their existing. It fails if the share grows past [rate withheld],
-- which would mean something changed upstream -- a scan writing bad durations, or a
-- new import of files whose metadata nobody checked.

select
    count(*)                                                   as titles,
    count(*) filter (where not is_runtime_plausible)           as implausible,
    round(100.0 * count(*) filter (where not is_runtime_plausible) / count(*), 3) as pct
from {{ ref('fct_library_title') }}
having count(*) filter (where not is_runtime_plausible) > count(*) * 0.01
