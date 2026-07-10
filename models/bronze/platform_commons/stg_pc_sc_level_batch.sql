{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'scLevelBatch') }}
)

select
    id::bigint as sc_level_batch_id,
    "academicYear"::text as academic_year,
    "scLevelId"::bigint as sc_level_id,
    "tenant"::bigint as tenant_id,
    "isActive"::boolean as is_active,
    "batchMigrationStatus"::text as batch_migration_status,
    "batchStatusCode"::text as batch_status_code,
    "trainerId"::bigint as trainer_id,
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
