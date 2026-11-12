select a.address, a.valid_from, b.valid_from as overlapping_from
from {{ ref('int_managed_origin_addresses') }} a
join {{ ref('int_managed_origin_addresses') }} b
  on a.address = b.address
 and (a.valid_from < b.valid_from or (a.valid_from = b.valid_from and a.origin_edge < b.origin_edge))
 and a.valid_from < coalesce(b.valid_until, 'infinity'::timestamptz)
 and b.valid_from < coalesce(a.valid_until, 'infinity'::timestamptz)
union all
select address, valid_from, valid_until
from {{ ref('int_managed_origin_addresses') }}
where valid_until <= valid_from
union all
select address, valid_from, valid_from
from {{ ref('int_managed_origin_addresses') }}
group by address, valid_from
having count(*) > 1
