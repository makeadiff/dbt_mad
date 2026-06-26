{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'university_universityName_bridge') }}
)

select
    "universityId"::bigint as university_id,
    "universityNameId"::bigint as university_name_id
from raw
