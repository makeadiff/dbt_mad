{{ config(materialized='table') }}

with raw_school_session_detail as (
    select * from {{ source('bubble_staging', 'school_session_detail') }}
)
select
    raw.response,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_session_detail raw
