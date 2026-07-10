{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'scLevelBatchEvents') }}
)

select
    id::bigint as batch_event_id,
    "startDate"::date as start_date,
    "endDate"::date as end_date,
    "reason"::text as reason,
    "type"::text as event_type,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
