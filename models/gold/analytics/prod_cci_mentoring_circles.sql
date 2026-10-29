{{ config(materialized='table') }}

-- CCI dashboard (E1): every mentoring circle for AY 2026-27, one row per slot shift. Shared base for
-- prod_cci_chapter_summary (Metrics 1 and 2) and prod_cci_weekly_sessions, so the circle rule lives
-- in one place. Not read by Dalgo.
--
-- Reaches into bronze (stg_pc_worknode_slot_shift and its user bridge) because no mart carries
-- circle grain (spec E1 §3.0).
--
-- Circle: an active slot shift (worknodeSlotShift, for_entity_type LTLD_SCLEVEL_BATCH = one section
-- x one subject) on a 2026-27 section that has active children (prod_cci_active_children).
-- Volunteer: an active, not deleted entry in the shift's user list; distinct per circle.
-- Started: at least one session held (prod_cci_session_dates) since 1 June 2026.
-- Weekly slot: the shift appears in worknodeSlot_worknodeSlotShiftList_bridge. The shift's own
-- worknode_slot_id is NOT used: it holds the school course id, not the slot (spec §6). The flag
-- only feeds the data-freshness gap; nothing here needs the slot day (spec §7 ruling 1).
--
-- Grain: one row per worknode_slot_shift_id.

with sections as (
    select distinct chapter_id, sc_level_batch_id
    from {{ ref('prod_cci_active_children') }}
),

shift_volunteers as (
    select
        b.worknode_slot_shift_id,
        count(distinct ul.owner_user_id) as volunteers,
        array_agg(distinct ul.owner_user_id) as volunteer_user_ids
    from {{ ref('stg_pc_worknode_slot_shift_user_bridge') }} b
    join {{ ref('stg_pc_worknode_slot_shift_user_list') }} ul
        on b.slot_shift_user_id = ul.slot_shift_user_id
    where ul.is_active
    group by 1
),

held as (
    select
        worknode_slot_shift_id,
        count(*) as sessions_held,
        count(*) filter (where session_date >= date '2026-10-01') as sessions_held_since_1_oct
    from {{ ref('prod_cci_session_dates') }}
    group by 1
)

select
    s.chapter_id,
    s.sc_level_batch_id,
    sh.worknode_slot_shift_id,
    coalesce(v.volunteers, 0) as volunteers,
    v.volunteer_user_ids,
    coalesce(h.sessions_held, 0) > 0 as is_started,
    coalesce(h.sessions_held_since_1_oct, 0) as sessions_held_since_1_oct,
    exists (
        select 1
        from {{ ref('stg_pc_worknode_slot_shift_list_bridge') }} sl
        where sl.worknode_slot_shift_id = sh.worknode_slot_shift_id
    ) as has_weekly_slot
from {{ ref('stg_pc_worknode_slot_shift') }} sh
join sections s
    on sh.for_entity_id = s.sc_level_batch_id
left join shift_volunteers v
    on sh.worknode_slot_shift_id = v.worknode_slot_shift_id
left join held h
    on sh.worknode_slot_shift_id = h.worknode_slot_shift_id
where sh.for_entity_type = 'LTLD_SCLEVEL_BATCH'
  and sh.is_active
