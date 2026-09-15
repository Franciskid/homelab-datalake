-- Every message, typed. `content` passes through unchanged -- full text, by the
-- owner's 2026-09-14 decision, and there is no cleaning to do to it.

with source as (

    select * from {{ source('raw', 'platform_messages') }}

)

select
    dt                                          as partition_date,
    id                                           as message_id,
    conversation_id,
    author_user_id,
    run_id,
    nullif(role, '')                            as role,
    content,
    metadata,
    created_at

from source
