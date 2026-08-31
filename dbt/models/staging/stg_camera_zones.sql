-- Attention-zone configuration, one row per zone and rule per day.
--
-- The reference table the event stream never had. Without it `rule_type = 'state'` is
-- the only clue that a zone is a gate, and there is no way at all to ask what a rule
-- was configured to do on the day an event fired against it.
--
-- A daily snapshot because the camera keeps no history for a zone: it answers for how
-- the zones are set up now. Backfilling would write today's configuration under an old
-- date and describe events by rules that did not exist when they fired.
--
-- Geometry never arrives. The polygon is a map of the property; the anchor and
-- reference *counts* travel instead, which is what a "is this zone calibrated"
-- question actually needs.

with source as (select * from {{ source('raw', 'camera_zones') }}),

renamed as (
    select
        dt                                          as snapshot_date,
        _extracted_at                               as extracted_at,
        id                                          as zone_id,
        "cameraId"                                  as camera_id,
        nullif(name, '')                            as zone_name,
        nullif(description, '')                     as zone_description,
        nullif(scope, '')                           as zone_scope,
        coalesce(enabled, false)                    as is_enabled,
        revision                                    as zone_revision,
        "geometryRevision"                          as geometry_revision,
        coalesce("anchorCount", 0)                  as anchor_count,
        coalesce("stateReferenceCount", 0)          as state_reference_count,
        coalesce("revisionCount", 0)                as revision_count,
        -- How often a person has corrected what this zone concluded. A zone with a
        -- lot of feedback is one the model keeps getting wrong, which no count of
        -- events would show.
        coalesce("feedbackCount", 0)                as feedback_count,
        "autoLearnAnchors"                          as does_auto_learn_anchors,
        "maxAnchors"                                as max_anchors,
        try_cast("createdAt" as timestamptz)        as created_at,
        nullif("createdByHash", '')                 as created_by_hash,
        nullif("createdByName", '')                 as created_by_name,
        try_cast("updatedAt" as timestamptz)        as updated_at,
        nullif("updatedByHash", '')                 as updated_by_hash,
        nullif("updatedByName", '')                 as updated_by_name,
        nullif("ruleId", '')                        as rule_id,
        nullif("ruleType", '')                      as rule_type,
        nullif("ruleName", '')                      as rule_name,
        "ruleEnabled"                               as is_rule_enabled,
        "ruleNotify"                                as does_rule_notify,
        "ruleCooldownSec"                           as rule_cooldown_seconds,
        nullif("ruleSensitivity", '')               as rule_sensitivity,
        -- The analysis prompt. Configuration of the model rather than anything anyone
        -- wrote about a person, and the only record of *why* a rule fires as it does.
        nullif("rulePrompt", '')                    as rule_prompt,
        "ruleTargets"                               as rule_targets,
        "ruleStates"                                as rule_states,
        "ruleAlertOn"                               as rule_alert_on,
        -- The geometry, lossless, as JSON. `shapes` is the polygon of the zone in
        -- normalised frame coordinates; the counts above answer most questions
        -- without parsing it.
        nullif("shapesJson", '')                    as shapes_json,
        nullif("ignoreShapesJson", '')              as ignore_shapes_json,
        nullif("anchorsJson", '')                   as anchors_json,
        nullif("stateReferencesJson", '')           as state_references_json,
        nullif("anchorPolicyJson", '')              as anchor_policy_json,
        -- Every past configuration, and every human correction to what the zone
        -- concluded. How a step change in event counts gets traced to the edit
        -- that caused it.
        nullif("revisionsJson", '')                 as revisions_json,
        nullif("feedbackJson", '')                  as feedback_json,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    (rule_type = 'state')                           as is_gate_rule,
    (rule_id is not null)                           as has_rule
from renamed
