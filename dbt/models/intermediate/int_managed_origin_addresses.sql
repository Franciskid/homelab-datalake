-- Private runtime inventory. No real addresses belong in source or build images.
-- service_origins.validated_origins validates it before warehouse loading; the
-- fingerprint includes it, so an inventory correction rebuilds historical rows.
{% set origins = env_var('DATALAKE_MANAGED_ORIGINS_JSON', '[]') %}
select
    json_extract_string(value, '$.address') as address,
    json_extract_string(value, '$.origin_edge') as origin_edge,
    cast(json_extract_string(value, '$.valid_from') as timestamptz) as valid_from,
    cast(json_extract_string(value, '$.valid_until') as timestamptz) as valid_until,
    json_extract_string(value, '$.evidence') as evidence
from json_each('{{ origins | replace("'", "''") }}')
