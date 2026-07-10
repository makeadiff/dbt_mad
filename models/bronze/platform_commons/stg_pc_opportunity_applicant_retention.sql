{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'opportunityApplicantRetention') }}
)

select
    id::bigint as retention_id,
    "userId"::bigint as user_id,
    "retentionFormYear"::text as retention_year,
    "retentionSentDate"::timestamp as retention_sent_date,
    status::text as retention_status,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
