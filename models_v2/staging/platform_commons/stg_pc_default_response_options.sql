{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'defaultResponseOptions') }}
)

select
    id::bigint as default_response_option_id,
    "optionsId"::bigint as options_id,
    "orderNo"::text as order_no,
    "weight"::text as weight,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
