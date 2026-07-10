{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'worknodeSlotShiftSubstitute') }}
)

select
    id::bigint as substitute_id,
    "byUser"::bigint as by_user_id,
    "forUser"::bigint as for_user_id,
    "forSlotShift"::bigint as for_slot_shift_id,
    "forDate"::timestamp as for_date,
    "originalSlotDate"::timestamp as original_slot_date,
    "requestedDate"::timestamp as requested_date,
    "requestingReason"::text as requesting_reason,
    "requestStatus"::text as request_status,
    "requestType"::text as request_type,
    "isSubstituteBySupervisor"::boolean as is_substitute_by_supervisor,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
