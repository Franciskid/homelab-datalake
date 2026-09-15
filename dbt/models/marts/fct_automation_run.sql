-- One row per run of an automation, with its step runs folded in: counts by outcome
-- for the ordinary "how did last night's run go" question, and a JSON array of every
-- step -- id, type, status, timing, error -- for the times that question needs the
-- detail. A bridge table of one row per step run would answer the second question no
-- better and would make the first one a join and a group by instead of a column.

with step_runs as (

    select
        automation_run_id,
        count(*)                                                    as steps_total,
        count(*) filter (where status = 'succeeded')                as steps_succeeded,
        count(*) filter (where status = 'failed')                   as steps_failed,
        count(*) filter (where status in ('pending', 'running'))     as steps_pending,
        max(finished_at)                                            as last_step_finished_at,
        -- json_group_array is a macro in DuckDB, not a true aggregate, and cannot
        -- take an ORDER BY -- array_agg can, so the ordering happens there and
        -- to_json turns the resulting LIST into the JSON array this column promises.
        to_json(array_agg(json_object(
            'step_id', step_id,
            'step_type', step_type,
            'step_label', step_label,
            'status', status,
            'started_at', started_at,
            'finished_at', finished_at,
            'error', error
        ) order by step_position))                                  as step_runs_json

    from {{ ref('stg_ai_platform_automation_step_runs') }}
    group by automation_run_id

)

select
    r.automation_run_id                         as automation_run_key,
    r.partition_date,
    r.created_at,
    r.started_at,
    r.finished_at,
    r.duration_ms,

    r.automation_id,
    coalesce(a.automation_name, r.automation_id)  as automation_name,
    r.dagster_run_id,
    r.status,

    coalesce(sr.steps_total, 0)                 as steps_total,
    coalesce(sr.steps_succeeded, 0)             as steps_succeeded,
    coalesce(sr.steps_failed, 0)                as steps_failed,
    coalesce(sr.steps_pending, 0)               as steps_pending,
    (coalesce(sr.steps_failed, 0) > 0)          as had_a_failed_step,
    sr.last_step_finished_at,
    sr.step_runs_json

from {{ ref('stg_ai_platform_automation_runs') }} r
left join {{ ref('stg_ai_platform_automations') }} a on r.automation_id = a.id
left join step_runs sr on r.automation_run_id = sr.automation_run_id
