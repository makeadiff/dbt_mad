{{ config(materialized='table') }}

-- E1 session-ops: resolves each batch (sc_level_batch) to its school/academic_year, every
-- slot_shift it's scheduled into, every volunteer tagged to that slot_shift, and whether the
-- slot_shift has ever had an attendance record captured. Built as a reusable base for E1 coverage-
-- style facts (fct_e1_school_coverage first, more may follow) so the batch->slot_shift->volunteer
-- join logic lives in one place instead of being repeated per fact.
-- Flow: stg_pc_sc_level_batch + stg_pc_sc_level_id + stg_pc_school_course (batch -> school)
--       + stg_pc_worknode_slot_shift + stg_pc_worknode_slot_shift_list_bridge + stg_pc_worknode_slot
--         (batch -> slot_shift -> slot)
--       + stg_pc_worknode_slot_shift_user_bridge + stg_pc_worknode_slot_shift_user_list
--         (slot_shift -> tagged volunteers)
--       + stg_pc_batch_attendance (slot_shift -> attendance existence) -> int_pc_batch_coverage
-- Grain: one row per (sc_level_batch_id, worknode_slot_shift_id, owner_user_id) -- a batch with no
-- slot_shift keeps one row (slot columns + volunteer null); a slot_shift with no tagged volunteer
-- keeps one row (owner_user_id null). Consumers doing count(distinct sc_level_batch_id) for a
-- "total sections" style metric, or count(distinct worknode_slot_shift_id)/count(distinct
-- owner_user_id) for slot/volunteer metrics, get correct counts regardless of which of these are
-- null on a given row.
-- No is_active filtering on batch/slot_shift/volunteer here -- unlike Bubble (E2), platform_commons
-- has no per-year archived/active dimension to branch on (see dim_school_academic_year_status for
-- the E2 equivalent, which doesn't exist for PC), so every batch/slot_shift/volunteer tied to a
-- given academic_year is counted regardless of its current is_active flag. This mirrors E2's own
-- total_sections, which likewise counts class_sections without any is_active filter.
-- has_attendance resolves for_slot_shift_id on the attendance record first, falling back to a
-- direct sc_level_batch_id match when the attendance row has no for_slot_shift_id -- the same
-- fallback pattern int_pc_class_ops_master/int_pc_child_attendance use for slot resolution.

with level_batch as (
    select * from {{ ref('stg_pc_sc_level_batch') }}
),

sc_level_id_table as (
    select * from {{ ref('stg_pc_sc_level_id') }}
),

school_course as (
    select * from {{ ref('stg_pc_school_course') }}
),

slot_shift as (
    select * from {{ ref('stg_pc_worknode_slot_shift') }}
    where for_entity_type = 'LTLD_SCLEVEL_BATCH'
),

slot_shift_list_bridge as (
    select * from {{ ref('stg_pc_worknode_slot_shift_list_bridge') }}
),

slot as (
    select * from {{ ref('stg_pc_worknode_slot') }}
),

batch_slot_shift as (
    select
        ss.for_entity_id as sc_level_batch_id,
        ss.worknode_slot_shift_id,
        ss.is_active as slot_shift_is_active,
        sl.worknode_slot_id,
        sl.is_active as slot_is_active
    from slot_shift ss
    left join slot_shift_list_bridge sslb on ss.worknode_slot_shift_id = sslb.worknode_slot_shift_id
    left join slot sl on sslb.worknode_slot_id = sl.worknode_slot_id
),

tagged_volunteers as (
    select
        wssub.worknode_slot_shift_id,
        ssul.owner_user_id,
        ssul.is_active as volunteer_tag_is_active
    from {{ ref('stg_pc_worknode_slot_shift_user_bridge') }} wssub
    join {{ ref('stg_pc_worknode_slot_shift_user_list') }} ssul
        on wssub.slot_shift_user_id = ssul.slot_shift_user_id
),

attendance_by_slot_shift as (
    select distinct for_slot_shift_id as worknode_slot_shift_id
    from {{ ref('stg_pc_batch_attendance') }}
    where for_slot_shift_id is not null
),

attendance_by_batch as (
    select distinct sc_level_batch_id
    from {{ ref('stg_pc_batch_attendance') }}
    where for_slot_shift_id is null
)

select
    sc.school_id,
    {{ clean_prefix('lb.academic_year') }} as academic_year,
    lb.sc_level_batch_id,
    lb.is_active as batch_is_active,
    bss.worknode_slot_shift_id,
    bss.slot_shift_is_active,
    bss.worknode_slot_id,
    bss.slot_is_active,
    tv.owner_user_id,
    tv.volunteer_tag_is_active,
    coalesce(abss.worknode_slot_shift_id is not null, abb.sc_level_batch_id is not null, false) as has_attendance
from level_batch lb
left join sc_level_id_table slit on lb.sc_level_id = slit.sc_level_id_table_id
left join school_course sc on slit.school_course_id = sc.school_course_id
left join batch_slot_shift bss on lb.sc_level_batch_id = bss.sc_level_batch_id
left join tagged_volunteers tv on bss.worknode_slot_shift_id = tv.worknode_slot_shift_id
left join attendance_by_slot_shift abss on bss.worknode_slot_shift_id = abss.worknode_slot_shift_id
left join attendance_by_batch abb on lb.sc_level_batch_id = abb.sc_level_batch_id
