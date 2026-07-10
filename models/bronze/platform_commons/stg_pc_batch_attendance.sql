{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'batchAttendance') }}
)

select
    id::bigint as batch_attendance_id,
    "aliasId"::text as alias_id,
    "scLevelBatchId"::bigint as sc_level_batch_id,
    date::text as attendance_date,
    "subjectCode"::text as subject_code,
    "capturedByUser"::text as captured_by_user_id,
    "forSlotShiftId"::bigint as for_slot_shift_id,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "tenant"::bigint as tenant_id,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
