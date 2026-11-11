{{ config(materialized='table') }}

-- DOTS numeracy assessment form, columns as landed (same shape as stg_dots__child_attendance).
-- Holds every round (baseline, midline 1-3, endline), drafts included.

with raw as (
    select * from {{ source('dots_raw', 'baselineAssessments') }}
)
select
    "id",
    "__v"::numeric as v,
    "_id",
    "main",
    "meta",
    "tags",
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
