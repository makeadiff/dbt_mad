{{ config(materialized='view') }}

select
    id::bigint as slot_shift_user_id,
    "ownerUserId"::text as owner_user_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from {{ source('pc_raw', 'worknodeSlotShiftUserList') }}
where "xIsDeleted" is false or "xIsDeleted" is null
