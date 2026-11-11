{{ config(materialized='table') }}

-- DOTS foundational life skills assessment form, columns as landed.
-- Until 2026-11 the raw feed had no main or tags column: Airbyte saved the stream's schema from the
-- first record (an empty draft, 1 Jun 2026) and dropped those fields from every later record. Dots
-- confirmed this on 10 Nov 2026; the fix is a schema refresh plus reset of this stream in Dalgo.
-- main and tags are selected only when the raw table has them, so this model builds before and
-- after that resync (null until then).

{%- set raw_rel = source('dots_raw', 'baselineAssessmentsFoundationalLifeSkill') -%}
{%- set raw_cols = [] -%}
{%- if execute -%}
    {%- for c in adapter.get_columns_in_relation(raw_rel) -%}
        {%- do raw_cols.append(c.name) -%}
    {%- endfor -%}
{%- endif %}

with raw as (
    select * from {{ raw_rel }}
)
select
    "id",
    "__v"::numeric as v,
    "_id",
    {% if 'main' in raw_cols %}"main"{% else %}null::jsonb as main{% endif %},
    "meta",
    {% if 'tags' in raw_cols %}"tags"{% else %}null::jsonb as tags{% endif %},
    "views",
    "shares",
    "ogStack" as og_stack,
    "comments",
    "metadata",
    "warnings",
    "reactions",
    "cloneStack" as clone_stack,
    "lastActivity" as last_activity,
    "kp_date_created",
    "kp_date_last_saved",
    "kp_published_status",
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw
