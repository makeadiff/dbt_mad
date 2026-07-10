{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'worknodeSlotShift') }}
)

select
    id::bigint as worknode_slot_shift_id,
    "parentEntityId"::bigint as worknode_slot_id,
    "forEntityId"::bigint as for_entity_id,
    "forEntityType"::text as for_entity_type,
    "isActive"::boolean as is_active,
    "supervisorId"::bigint as supervisor_id,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
