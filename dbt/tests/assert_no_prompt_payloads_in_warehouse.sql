-- LiteLLM's ledger carries messages, response and proxy_server_request: the full
-- prompt and completion payloads. They are empty on this deployment because payload
-- logging is off, and the extractor names its columns explicitly so that switching it
-- on cannot start ingesting conversations silently.
--
-- This is the check that the explicit column list stayed explicit. It fails if any of
-- those names ever appear as a column on the fact table -- which is what a `select *`
-- refactor would produce.

with columns_present as (

    select lower(column_name) as column_name
    from information_schema.columns
    where lower(table_name) = 'fct_llm_call'

)

select column_name
from columns_present
where column_name in ('messages', 'response', 'proxy_server_request', 'api_key',
                      'requester_ip_address', 'prompt', 'completion_text')
