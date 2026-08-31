-- The zone and rule a camera event points at.
--
-- The lookup `fct_camera_event.rule_id` and `zone_id` had nothing to join to. Without
-- it the only way to know a zone is a gate is that its events happen to carry
-- `rule_type = 'state'`, which is inference from the data rather than a fact about the
-- configuration -- and it says nothing at all about a zone that has fired no events.
--
-- Grain is the latest snapshot per zone and rule. History is deliberately not kept
-- here: the camera has none to give, so a slowly-changing dimension over one day of
-- observations would be a shape implying a depth that does not exist.

with zones as (select * from {{ ref('stg_camera_zones') }}),

latest as (

    select
        *,
        row_number() over (
            partition by zone_id, coalesce(rule_id, '')
            order by snapshot_date desc, extracted_at desc
        ) as recency
    from zones

)

select
    -- A zone with several rules is several rows, so neither id alone is the key.
    zone_id || '|' || coalesce(rule_id, '')             as zone_rule_key,
    zone_id,
    rule_id,
    camera_id,
    zone_name,
    zone_description,
    zone_scope,
    is_enabled,
    zone_revision,
    geometry_revision,
    anchor_count,
    state_reference_count,
    revision_count,
    feedback_count,
    (anchor_count > 0)                                  as is_calibrated,
    (feedback_count > 0)                                as has_been_corrected,
    does_auto_learn_anchors,
    max_anchors,
    created_at,
    created_by_name,
    created_by_hash,
    updated_at,
    updated_by_name,
    updated_by_hash,

    rule_type,
    rule_name,
    is_rule_enabled,
    is_gate_rule,
    does_rule_notify,
    rule_cooldown_seconds,
    rule_sensitivity,
    rule_prompt,
    rule_targets,
    rule_states,
    rule_alert_on,

    snapshot_date                                       as observed_on,
    extracted_at

from latest
where recency = 1
