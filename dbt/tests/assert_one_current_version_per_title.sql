-- Exactly one current version per title. Two would double every "what is in the
-- library now" count; zero would make the title vanish from it entirely.

select item_id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_title_scd2') }}
group by item_id
having count(*) filter (where is_current) <> 1
