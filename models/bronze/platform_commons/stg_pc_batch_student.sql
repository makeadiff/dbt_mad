{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'batchStudent') }}
)

select
    id::bigint as batch_student_id,
    "scLevelBatchId"::bigint as sc_level_batch_id,
    "studentId"::bigint as student_id,
    "batchStudentStatus"::text as batch_student_status,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
