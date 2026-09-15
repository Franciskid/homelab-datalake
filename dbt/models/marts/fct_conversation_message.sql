-- One row per message in every conversation, full text, by the owner's 2026-09-14
-- decision -- see IMPLEMENTATION_PLAN.md section 15. The conversation's title and agent are joined in so a
-- listing reads like a transcript rather than a bag of rows keyed by ids nobody
-- recognises.

select
    m.message_id                                as message_key,
    m.partition_date,
    m.created_at,
    date_trunc('hour', m.created_at)            as created_hour,

    m.conversation_id,
    c.title                                     as conversation_title,
    c.agent_id,
    coalesce(ag.agent_name, c.agent_id)         as agent_name,
    c.owner_user_id                             as conversation_owner_user_id,

    m.author_user_id,
    m.run_id,
    m.role,
    m.content,
    length(m.content)                           as content_length,
    m.metadata

from {{ ref('stg_ai_platform_messages') }} m
left join {{ ref('stg_ai_platform_conversations') }} c on m.conversation_id = c.conversation_id
left join {{ ref('stg_ai_platform_agents') }} ag on c.agent_id = ag.id
