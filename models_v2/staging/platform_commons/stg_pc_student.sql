{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'student') }}
)

select
    id::bigint as student_id,
    "iEId"::bigint as ie_id,
    "studentSysGenCode"::text as student_code,
    "studentStatus"::text as student_status,
    "isActive"::boolean as is_active,
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
