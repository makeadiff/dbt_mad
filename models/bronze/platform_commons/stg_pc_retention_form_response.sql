{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'retentionFormResponse') }}
)

select
    id::bigint as retention_form_response_id,
    "opportunityApplicantRetentionId"::bigint as retention_id,
    "preferredWorkNodeId"::bigint as preferred_work_node_id,
    "preferredRole"::text as preferred_role,
    case 
        when upper("retentionDecision") = 'RETENTION_DECISION.YES' then 'yes'
        when upper("retentionDecision") = 'RETENTION_DECISION.NO' then 'no'
        else null
    end as willing_to_continue,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
