-- Every email the platform has sent or attempted to send, typed. `recipients` and
-- `attachments` stay as JSON text; recipient_count below is what a listing actually
-- wants to group or filter by without parsing them.

with source as (

    select * from {{ source('raw', 'platform_email_sends') }}

)

select
    dt                                          as partition_date,
    id                                           as email_send_id,
    owner_user_id,
    run_id,
    approval_id,
    template_script_id,
    template_version,
    nullif(subject, '')                         as subject,
    recipients,
    -- json_array_length raises on a scalar or an object; recipients is documented as
    -- an array but nothing upstream enforces that, so a malformed value reports zero
    -- recipients rather than failing the whole model.
    case when json_valid(recipients) and json_type(recipients) = 'ARRAY'
         then json_array_length(recipients)
         else 0
    end                                          as recipient_count,
    attachments,
    case when json_valid(attachments) and json_type(attachments) = 'ARRAY'
         then json_array_length(attachments)
         else 0
    end                                          as attachment_count,
    coalesce(nullif(status, ''), 'unknown')     as status,
    nullif(failure_reason, '')                  as failure_reason,
    created_at

from source
