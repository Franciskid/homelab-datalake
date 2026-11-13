-- One row per change to a global setting held by fraserv-infra's infra-control: who
-- changed it (the owner from the Infra tab, or an agent the owner approved), through
-- which client, from what to what, and why. `import` rows are the shadow mirror copying
-- the AI platform's settings before the cutover.

select
    change_id,
    cast(changed_at as date)                            as partition_date,
    setting_key,
    setting_namespace,
    from_json,
    to_json,
    actor,
    actor_kind,
    agent,
    run_id,
    via,
    reason,
    changed_at,
    reverts_change_id,
    reverts_change_id is not null                       as is_revert

from {{ ref('stg_infra_control_config_changes') }}
