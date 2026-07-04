{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'options') }}
)

select
    id::bigint as options_id,
    "optionCode"::text as option_code,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
