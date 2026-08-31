-- Camera events, typed.
--
-- Two things happen here that are worth naming.
--
-- **The subject counts are flattened.** They arrive as a struct because that is the
-- shape the camera sends, and DuckDB reads it natively -- but `counts.person` does not
-- survive an export to a spreadsheet, and the spreadsheet is where these numbers are
-- actually read. So each subject gets a column, and `count_total` gets one too: every
-- graph of this data is a sum over all four, and summing four columns by hand is how a
-- headline comes to disagree with the rows beneath it.
--
-- **A correction is unpacked, not resolved.** The camera writes a person's answer over
-- the machine's on the row itself, so the top-level columns already *are* the corrected
-- truth; `correction.previous` is what the machine had said. Both travel. Nothing is
-- chosen here -- the mart is where one is presented as the answer and the other as
-- provenance.

with source as (select * from {{ source('raw', 'camera_events') }}),

renamed as (
    select
        dt                                          as partition_date,
        _extracted_at                               as extracted_at,
        id                                          as event_id,
        "cameraId"                                  as camera_id,
        "zoneId"                                    as zone_id,
        nullif("zoneName", '')                      as zone_name,
        nullif("ruleId", '')                        as rule_id,
        nullif("ruleType", '')                      as rule_type,
        nullif(lifecycle, '')                       as lifecycle,
        -- The export sends ISO-8601 strings; cast once, here, so nothing downstream
        -- has to guess the format or the zone.
        try_cast("startedAt"       as timestamptz)  as started_at,
        -- When it actually happened, which a replay or a repair moves away from
        -- `startedAt`. The camera's own report ranks on this, falling back to
        -- `startedAt`, and so does anything here that wants to agree with it.
        try_cast("occurredAt"      as timestamptz)  as occurred_at,
        try_cast("completedAt"     as timestamptz)  as completed_at,
        try_cast("stateObservedAt" as timestamptz)  as state_observed_at,
        nullif("stateSource", '')                   as state_source,
        nullif("clipId", '')                        as clip_id,
        nullif("transitionClipId", '')              as transition_clip_id,
        nullif(direction, '')                       as direction,
        nullif(state, '')                           as zone_state,
        nullif("previousState", '')                 as previous_zone_state,

        -- Null is a real value and is not a low score: it means the model declined to
        -- commit. Coalescing it to 0 would turn a refusal into a confident "no".
        confidence,
        coalesce(unusual, false)                    as is_unusual,
        coalesce("shouldNotify", false)             as should_notify,
        coalesce(dismissed, false)                  as is_dismissed,
        nullif("dismissalReason", '')               as dismissal_reason,
        nullif("duplicateOf", '')                   as duplicate_of,
        nullif(summary, '')                         as summary,

        -- Null and zero are different here too: an event nobody counted is not an
        -- event that counted nobody.
        counts.person                               as count_person,
        counts.vehicle                              as count_vehicle,
        counts.animal                               as count_animal,
        counts.other                                as count_other,
        case when counts is null then null
             else coalesce(counts.person,  0) + coalesce(counts.vehicle, 0)
                + coalesce(counts.animal, 0) + coalesce(counts.other,   0)
        end                                         as count_total,

        (correction is not null)                    as was_corrected,
        try_cast(correction."correctedAt" as timestamptz) as corrected_at,
        nullif(correction."correctedByName", '')    as corrected_by_name,
        nullif(correction."correctedByHash", '')    as corrected_by_hash,
        -- What the machine said before a person overwrote it. Prefixed `machine_` so
        -- a reader can never mistake one for the other in a wide result.
        nullif(correction.previous.lifecycle, '')   as machine_lifecycle,
        nullif(correction.previous.direction, '')   as machine_direction,
        nullif(correction.previous.state, '')       as machine_zone_state,
        nullif(correction.previous."previousState", '') as machine_previous_zone_state,
        try_cast(correction.previous."stateObservedAt" as timestamptz)
                                                    as machine_state_observed_at,
        nullif(correction.previous."dismissalReason", '') as machine_dismissal_reason,
        nullif(correction.previous."duplicateOf", '') as machine_duplicate_of,
        nullif(correction.previous.summary, '')     as machine_summary,
        correction.previous.confidence              as machine_confidence,
        correction.previous."shouldNotify"          as machine_should_notify,
        correction.previous.counts.person           as machine_count_person,
        correction.previous.counts.vehicle          as machine_count_vehicle,
        correction.previous.counts.animal           as machine_count_animal,
        correction.previous.counts.other            as machine_count_other,

        -- Rare provenance blocks, kept as JSON. A row that disagrees with its
        -- neighbours is explained by one of these or by nothing at all, and "by
        -- nothing at all" is a bug report.
        nullif(backfill, '')                        as backfill_json,
        nullif(repair, '')                          as repair_json,
        nullif(restoration, '')                     as restoration_json,
        nullif("timingCorrection", '')              as timing_correction_json,
        nullif(_unmapped, '')                       as unmapped_json
    from source
)

select
    *,
    (lifecycle = 'confirmed')                       as is_confirmed,
    (lifecycle = 'candidate')                       as is_awaiting_analysis,
    case when completed_at is not null and started_at is not null
         then datediff('second', started_at, completed_at) end as duration_seconds,
    -- The instant to rank an event by, resolved once. The camera's own report does
    -- `occurredAt || startedAt`; anything here that wants to agree with it must do the
    -- same, and doing it in one place is how that stays true.
    coalesce(occurred_at, started_at)               as happened_at
from renamed
