-- One row per email the platform has sent or attempted to send.

select
    e.email_send_id                             as email_send_key,
    e.partition_date,
    e.created_at,
    date_trunc('hour', e.created_at)            as created_hour,

    e.owner_user_id,
    e.run_id,
    e.approval_id,
    (e.approval_id is not null)                 as was_approved,
    e.template_script_id,
    e.template_version,

    e.subject,
    e.recipients,
    e.recipient_count,
    e.attachments,
    e.attachment_count,
    (e.attachment_count > 0)                    as has_attachments,

    e.status,
    (e.status = 'sent')                         as was_sent,
    e.failure_reason

from {{ ref('stg_ai_platform_email_sends') }} e
