select
    dt        as snapshot_date,
    item_id,
    genre
from {{ source('raw', 'jellyfin_title_genres') }}
where nullif(trim(genre), '') is not null
