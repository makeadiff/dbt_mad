{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'worknodeSlot_worknodeSlotShiftList_bridge') }}
)

select
    "worknodeSlotId"::bigint            as worknode_slot_id,
    -- Note: worknodeSlotShiftListId is now null in source; worknodeSlotShiftId is the live column
    "worknodeSlotShiftId"::bigint       as worknode_slot_shift_id
from raw
