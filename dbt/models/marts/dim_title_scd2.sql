-- Type-2 history over the daily snapshots.
--
-- One row per title per run of unchanged attributes, with the window it was true for.
-- This is what the snapshots are actually for: when a title arrived, when a rating
-- moved, when a file was re-encoded to a different codec or resolution.
--
-- Built from snapshots rather than dbt's snapshot materialisation on purpose. dbt
-- snapshots mutate a table in place, which makes them stateful and impossible to
-- rebuild from the lake; this is a pure function of the Parquet partitions, so
-- dropping the warehouse and rebuilding gives byte-identical history.

with snapshots as (

    select
        item_id,
        snapshot_date,
        title,
        community_rating,
        video_codec,
        resolution_class,
        runtime_minutes
    from {{ ref('stg_jellyfin_titles') }}

),

-- A change is any difference from the previous snapshot of the same title. Comparing
-- with `is distinct from` rather than `<>` so a rating going null counts as a change:
-- `null <> 7.1` is null, which would silently drop the row from the change set.
flagged as (

    select
        *,
        case when
            title             is distinct from lag(title)            over w
         or community_rating  is distinct from lag(community_rating) over w
         or video_codec       is distinct from lag(video_codec)      over w
         or resolution_class  is distinct from lag(resolution_class) over w
         or runtime_minutes   is distinct from lag(runtime_minutes)  over w
        then 1 else 0 end as is_change
    from snapshots
    window w as (partition by item_id order by snapshot_date)

),

versioned as (

    select *, sum(is_change) over (partition by item_id order by snapshot_date) as version
    from flagged

)

select
    {{ dbt_utils.generate_surrogate_key(['item_id', 'version']) }} as title_version_key,
    item_id,
    version,
    min(snapshot_date)                                            as valid_from,
    -- The current version has no end. A sentinel date would make it sort and filter
    -- like any other row, which is how "as of" queries quietly return the wrong
    -- version.
    case when max(snapshot_date) = (select max(snapshot_date) from snapshots)
         then null else max(snapshot_date) end                    as valid_to,
    max(snapshot_date) = (select max(snapshot_date) from snapshots) as is_current,
    any_value(title)                                              as title,
    any_value(community_rating)                                   as community_rating,
    any_value(video_codec)                                        as video_codec,
    any_value(resolution_class)                                   as resolution_class,
    any_value(runtime_minutes)                                    as runtime_minutes
from versioned
group by item_id, version
