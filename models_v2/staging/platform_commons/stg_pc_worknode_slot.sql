{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'worknodeSlot') }}
)

select
    id::bigint as worknode_slot_id,
    name::text as slot_name,
    "dayOfWeek"::text as day_of_week,
    "startTime"::timestamp as start_time,
    "endTime"::timestamp as end_time,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
