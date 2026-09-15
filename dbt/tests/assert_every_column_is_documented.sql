-- Every column in every model and source has a description.
--
-- The workbench renders these in the table panel and hands them to the SQL editor's
-- autocomplete, so an undescribed column is not a documentation gap -- it is a blank
-- cell in a tool somebody is using to decide what a number means. 430 of them were
-- written in one pass; without a test the 431st arrives undescribed and nothing says so.
--
-- **It reads the warehouse, not the YAML.** `adapter.get_columns_in_relation` asks the
-- database what the model actually produced. A test that compared the YAML against
-- itself would pass for a column added to a `select *` and never written down, which is
-- exactly how this drifts.
--
-- The `depends_on` lines are what make it run last. A singular test with no refs is
-- scheduled in the first batch, where it would inspect whatever the previous build left
-- behind. Naming a leaf model pulls in everything upstream of it; nine leaves are named
-- because the Jellyfin playback/activity/account branch, added 2026-09-14, is not
-- upstream of any of the original six.
--
-- Sources are checked too. They are the layer with the most columns and the least
-- natural pressure to document -- nobody writes a source column, so nobody notices one
-- appearing.

-- depends_on: {{ ref('agg_traffic_daily') }}
-- depends_on: {{ ref('agg_llm_spend_daily') }}
-- depends_on: {{ ref('agg_camera_activity_daily') }}
-- depends_on: {{ ref('bridge_title_genre') }}
-- depends_on: {{ ref('dim_title_scd2') }}
-- depends_on: {{ ref('dim_genre') }}
-- depends_on: {{ ref('fct_jellyfin_playback') }}
-- depends_on: {{ ref('fct_jellyfin_activity') }}
-- depends_on: {{ ref('dim_jellyfin_user') }}

{%- set undocumented = [] -%}

{%- if execute -%}
    {%- set checked = [] -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
        {%- do checked.append(node) -%}
    {%- endfor -%}
    {%- for node in graph.sources.values() -%}
        {%- do checked.append(node) -%}
    {%- endfor -%}

    {%- for node in checked -%}
        {%- set relation = adapter.get_relation(
                database=node.database,
                schema=node.schema,
                identifier=node.alias or node.identifier or node.name) -%}
        {#- A relation that is not there is not this test's business. On a fresh
            warehouse a source has not been loaded yet, and reporting that as an
            undocumented column would hide the real message. -#}
        {%- if relation is not none -%}
            {%- for column in adapter.get_columns_in_relation(relation) -%}
                {%- set documented = node.columns.get(column.name) -%}
                {%- if not documented or not (documented.description | trim) -%}
                    {%- do undocumented.append((node.name, column.name)) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{% if undocumented %}
select * from (
    values
    {%- for model_name, column_name in undocumented %}
        ('{{ model_name | replace("'", "''") }}', '{{ column_name | replace("'", "''") }}'){{ "," if not loop.last }}
    {%- endfor %}
) as t(model_name, undocumented_column)
{% else %}
-- Nothing undocumented. A test must still be a query, so this is one that
-- returns no rows by construction.
select
    cast(null as varchar) as model_name,
    cast(null as varchar) as undocumented_column
where false
{% endif %}
