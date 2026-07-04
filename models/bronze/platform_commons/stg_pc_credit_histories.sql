{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'creditHistories') }}
)

select
    id::bigint as credit_history_id,
    "points"::text as points,
    "initialPoints"::text as initial_points,
    "finalPoints"::text as final_points,
    "reason"::text as reason,
    "pointType"::text as point_type,
    "forSlotShiftId"::bigint as for_slot_shift_id,
    "forDate"::timestamp as for_date,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
