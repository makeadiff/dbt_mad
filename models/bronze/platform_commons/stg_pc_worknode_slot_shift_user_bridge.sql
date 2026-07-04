{{ config(materialized='view') }}

select
    "worknodeSlotShiftId"::bigint as worknode_slot_shift_id,
    "worknodeSlotShiftUserListId"::bigint as slot_shift_user_id
from {{ source('pc_raw', 'worknodeSlotShift_worknodeSlotShiftUserList_bridge') }}
