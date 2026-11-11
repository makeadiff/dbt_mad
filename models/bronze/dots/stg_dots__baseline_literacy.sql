{{ config(materialized='table') }}

-- DOTS foundational literacy assessment form, columns as landed. No tags column in the raw feed.
-- Holds every round (baseline, midline 1-3, endline), drafts included.

with raw as (
    select * from {{ source('dots_raw', 'baselineAssessmentsFoundationalLiteracyP') }}
)
select
    "id",
    "__v"::numeric as v,
    "_id",
    "main",
    "meta",
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
