-- The library must hold exactly one row per title per snapshot.
--
-- fct_library_title left-joins TMDB reference data. If that table ever held two rows
-- for one id, the join would fan out and every count, runtime total and average in the
-- library marts would be quietly multiplied. There would be no error -- just a library
-- that appears to have grown overnight.
--
-- stg_tmdb_titles has a uniqueness test on its key, which is the direct guard. This is
-- the one that checks the property people actually care about, at the grain they care
-- about it, so it still fails if the join gains a second condition later.

select
    item_id,
    snapshot_date,
    count(*) as rows_for_one_title
from {{ ref('fct_library_title') }}
group by 1, 2
having count(*) > 1
