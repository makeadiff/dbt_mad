{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'school_schoolName_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "schoolId"::bigint as school_id,
    "schoolNameId"::bigint as school_name_id
from raw
