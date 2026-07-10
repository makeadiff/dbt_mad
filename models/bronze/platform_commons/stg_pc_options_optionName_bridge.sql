{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'options_optionName_bridge') }}
)

select
    "optionsId"::bigint as options_id,
    "optionNameId"::bigint as option_name_id
from raw
