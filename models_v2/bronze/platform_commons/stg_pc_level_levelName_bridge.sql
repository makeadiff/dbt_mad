{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'level_levelName_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "levelId"::bigint as level_id,
    "levelNameId"::bigint as level_name_id
from raw
