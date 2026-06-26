{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'contact') }}
)

select
    id::bigint as contact_id,
    "contactTypeDataCode"::text as contact_type,
    "contactValue"::text as contact_value,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
