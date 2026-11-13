-- infra-control's settings change log. Every daily snapshot holds the whole log, so the
-- newest copy of each change is taken.

with source as (

    select * from {{ source('raw', 'infra_control_config_changes') }}
    qualify row_number() over (partition by change_id order by dt desc) = 1

)

select
    change_id,
    key                                                 as setting_key,
    split_part(key, '.', 1)                             as setting_namespace,
    from_json,
    to_json,
    actor,
    actor_kind,
    via,
    nullif(reason, '')                                  as reason,
    nullif(agent, '')                                   as agent,
    nullif(run_id, '')                                  as run_id,
    changed_at,
    reverts                                             as reverts_change_id

from source
