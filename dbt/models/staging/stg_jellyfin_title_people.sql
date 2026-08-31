-- Cast and crew credits, typed. One row per person per title.
--
-- Public film metadata -- who acted in and made a film -- and not anything about
-- anybody using this library. Requested from the API but never stored until now, so
-- "which director is most represented here" had no table to read.

with source as (select * from {{ source('raw', 'jellyfin_title_people') }}),

renamed as (
    select
        dt                                          as snapshot_date,
        item_id,
        person_id,
        nullif(name, '')                            as person_name,
        nullif(person_type, '')                     as person_type,
        nullif(role, '')                            as role,
        sort_order
    from source
)

select
    *,
    (person_type = 'Actor')                         as is_cast,
    -- Top billing is 0. The distinction between a lead and a walk-on is the reason
    -- the order is kept rather than the set of names alone.
    (sort_order < 5)                                as is_top_billed
from renamed
