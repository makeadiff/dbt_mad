{{ config(materialized='table') }}

-- DO dashboard (E2): one row per active E2 chapter for AY 2026-27. Metric 1 (volunteer
-- requirement met, 1:5 and 2:5), Metric 2 (sessions commenced) and child cover. Metric 4 joins
-- this model later.
--
-- Reaches into silver (int_bubble__slot, int_bubble__slot_class_section,
-- int_bubble__slot_class_section_volunteer, int_bubble__class_section, int_bubble__school_class,
-- int_bubble__child_class, int_bubble__child_class_section, int_bubble__child_removal_log) rather
-- than marts-only, because no mart carries circle-grain (section x slot) data with the
-- active/removed flags at every level. If the later CHO volunteer and child views need the same
-- circles, push the circle and child CTEs down into a new fct_ then.
--
-- Live mentoring circle (with a slot): one distinct (slot_id, class_section_id) pair whose
-- slot_class_section row, slot and class_section are all active and not removed, with the slot in
-- AY 2026-27. The school_class flag is deliberately NOT used: circles mix grades, so a live section
-- can sit under a class flagged removed (443's 8th) or have no class at all (554's sections,
-- school_class_id empty in sessionops_raw). When a class is truly removed its sections are removed
-- too (604).
--
-- A mentoring circle is a live class section with active children. Circles with a slot count per
-- section-slot pair (a section with two slots is two circles: 554 = 18 sections, 26 circles, as the
-- frontend shows); pairs whose section has no active children are dropped. Circles with no slot
-- count per section. mentoring_circles_total adds the two.
--
-- Mentoring circle with no slot: a live class section (active, not removed, in the chapter's
-- 2026-27 year) with >= 1 active child but no live slot link, one per section. In Bubble a section
-- only gets a slot link once a volunteer is placed, so these are the real volunteer gap (604: 4).
-- Sections with no active children are ignored.
--
-- Active child: child active and not removed; child_class not removed, on a not-removed
-- school_class in the chapter's 2026-27 year; no not-removed child_removal_log row dated after
-- that enrolment. Verified against the frontend at 427 / 604 / 56 / 554.
--
-- Child cover: a child's sections are their not-removed child_class_section links to live
-- sections at the chapter, with no match to the child's enrolled class. Each child lands in the
-- worst group that applies: in no class section > in a circle with no slot > in a circle with no
-- volunteer > covered. The four groups sum to active_children.
--
-- Metric 2: a circle with a slot has started when >= 1 session was held on any row of its
-- section-slot pair in AY 2026-27: a DOTS volunteer attendance record marked present, or absent
-- with a substitute (fct_e2_volunteer_attendance_by_slot_date). DOTS holds no planned or cancelled
-- sessions, so those can't count. Circles with no slot are left out of Metric 2.
--
-- Grain: one row per chapter_id (62 active E2 chapters in dim_chapter_mapping).

with chapters as (
    select chapter_id, chapter_name, city_name, co_name, cho_name, engine
    from {{ ref('dim_chapter_mapping') }}
    where engine = 'E2'
      and chapter_status = 'Active'
),

ay_status as (
    select school_id::text as chapter_id, is_ay_active
    from {{ ref('dim_school_academic_year_status') }}
    where academic_year = '2026-2027'
),

school_years as (
    select say.school_academic_year_id, say.school_id::text as chapter_id
    from {{ ref('int_bubble__school_academic_year') }} say
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    where ay.label = '2026-2027'
),

live_slots as (
    select s.slot_id, sy.chapter_id
    from {{ ref('int_bubble__slot') }} s
    join school_years sy
        on s.school_academic_year_id = sy.school_academic_year_id
    where s.is_active and not s.is_removed
),

live_sections as (
    select cs.class_section_id, sy.chapter_id
    from {{ ref('int_bubble__class_section') }} cs
    join school_years sy
        on cs.school_academic_year_id = sy.school_academic_year_id
    where cs.is_active and not cs.is_removed
),

live_scs_rows as (
    select scs.slot_class_section_id, scs.slot_id, scs.class_section_id, ls.chapter_id
    from {{ ref('int_bubble__slot_class_section') }} scs
    join live_slots ls
        on scs.slot_id = ls.slot_id
    join {{ ref('int_bubble__class_section') }} cs
        on scs.class_section_id = cs.class_section_id
    where scs.is_active and not scs.is_removed
      and cs.is_active and not cs.is_removed
),

-- One circle per section-slot pair; a volunteer on any of the pair's rows fills it.
circles as (
    select
        r.chapter_id,
        r.slot_id,
        r.class_section_id,
        count(distinct v.volunteer_id) as volunteers
    from live_scs_rows r
    left join {{ ref('int_bubble__slot_class_section_volunteer') }} v
        on r.slot_class_section_id = v.slot_class_section_id
       and v.is_active and not v.is_removed
    group by 1, 2, 3
),

-- Per live section: does it have a slot link, and does any of its circles lack a volunteer.
section_status as (
    select
        s.chapter_id,
        s.class_section_id,
        count(c.slot_id) > 0 as has_slot,
        count(c.slot_id) filter (where c.volunteers = 0) > 0 as has_circle_without_volunteer
    from live_sections s
    left join circles c
        on s.class_section_id = c.class_section_id
       and s.chapter_id = c.chapter_id
    group by 1, 2
),

slot_counts as (
    select chapter_id, count(*) as total_slots
    from live_slots
    group by 1
),

latest_removal as (
    select child_id, max(created_date) as last_removal_date
    from {{ ref('int_bubble__child_removal_log') }}
    where not is_removed
    group by 1
),

active_children as (
    select distinct sy.chapter_id, cc.child_id
    from {{ ref('int_bubble__child_class') }} cc
    join {{ ref('int_bubble__school_class') }} sc
        on cc.school_class_id = sc.school_class_id
    join school_years sy
        on sc.school_academic_year_id = sy.school_academic_year_id
    join {{ ref('int_bubble__children') }} c
        on cc.child_id = c.child_id
    left join latest_removal rl
        on cc.child_id = rl.child_id
    where c.is_active and not c.is_removed
      and not cc.is_removed
      and not sc.is_removed
      and (rl.last_removal_date is null or rl.last_removal_date <= cc.created_date)
),

child_sections as (
    select distinct ac.chapter_id, ac.child_id, ss.class_section_id, ss.has_slot, ss.has_circle_without_volunteer
    from active_children ac
    join {{ ref('int_bubble__child_class_section') }} ccs
        on ac.child_id = ccs.child_id
       and not ccs.is_removed
    join section_status ss
        on ccs.class_section_id = ss.class_section_id
       and ac.chapter_id = ss.chapter_id
),

held_pairs as (
    select distinct scs.slot_id, scs.class_section_id
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }} a
    join {{ ref('int_bubble__slot_class_section') }} scs
        on a.slot_class_section_id = scs.slot_class_section_id
    where a.academic_year = '2026-2027'
      and (upper(trim(a.attendance)) = 'TRUE' or a.is_substitute)
),

-- Circles with a slot, dropping pairs whose section has no active children.
counted_circles as (
    select
        c.*,
        exists (
            select 1 from held_pairs h
            where h.slot_id = c.slot_id
              and h.class_section_id = c.class_section_id
        ) as started
    from circles c
    where exists (
        select 1 from child_sections cs
        where cs.chapter_id = c.chapter_id
          and cs.class_section_id = c.class_section_id
    )
),

circle_counts as (
    select
        chapter_id,
        count(*) as mentoring_circles_with_slot,
        count(*) filter (where volunteers = 0) as mentoring_circles_0_volunteers,
        count(*) filter (where volunteers = 1) as mentoring_circles_1_volunteer,
        count(*) filter (where volunteers >= 2) as mentoring_circles_2plus_volunteers,
        count(*) filter (where started) as mentoring_circles_started,
        count(*) filter (where not started) as mentoring_circles_not_started
    from counted_circles
    group by 1
),

no_slot_circle_counts as (
    select chapter_id, count(distinct class_section_id) as mentoring_circles_no_slot
    from child_sections
    where not has_slot
    group by 1
),

-- Worst group per child: no section > no slot > no volunteer > covered.
child_groups as (
    select
        ac.chapter_id,
        ac.child_id,
        case
            when count(cs.class_section_id) = 0 then 'no_section'
            when bool_or(not cs.has_slot) then 'no_slot'
            when bool_or(cs.has_circle_without_volunteer) then 'no_volunteer'
            else 'covered'
        end as cover_group
    from active_children ac
    left join child_sections cs
        on ac.chapter_id = cs.chapter_id
       and ac.child_id = cs.child_id
    group by 1, 2
),

child_counts as (
    select
        chapter_id,
        count(*) as active_children,
        count(*) filter (where cover_group = 'covered') as children_covered,
        count(*) filter (where cover_group = 'no_volunteer') as children_in_circle_no_volunteer,
        count(*) filter (where cover_group = 'no_slot') as children_in_circle_no_slot,
        count(*) filter (where cover_group = 'no_section') as children_in_no_class_section
    from child_groups
    group by 1
),

joined as (
    select
        ch.engine,
        ch.chapter_id,
        ch.chapter_name,
        ch.city_name as city,
        ch.co_name,
        ch.cho_name,
        null::text as district,
        null::text as do_name,
        '2026-2027' as academic_year,
        coalesce(ays.is_ay_active, false) as has_data_this_year,
        coalesce(sl.total_slots, 0) as total_slots,
        coalesce(cc.mentoring_circles_with_slot, 0) as mentoring_circles_with_slot,
        coalesce(ns.mentoring_circles_no_slot, 0) as mentoring_circles_no_slot,
        coalesce(cc.mentoring_circles_0_volunteers, 0) as mentoring_circles_0_volunteers,
        coalesce(cc.mentoring_circles_1_volunteer, 0) as mentoring_circles_1_volunteer,
        coalesce(cc.mentoring_circles_2plus_volunteers, 0) as mentoring_circles_2plus_volunteers,
        coalesce(cc.mentoring_circles_started, 0) as mentoring_circles_started,
        coalesce(cc.mentoring_circles_not_started, 0) as mentoring_circles_not_started,
        coalesce(kc.active_children, 0) as active_children,
        coalesce(kc.children_covered, 0) as children_covered,
        coalesce(kc.children_in_circle_no_volunteer, 0) as children_in_circle_no_volunteer,
        coalesce(kc.children_in_circle_no_slot, 0) as children_in_circle_no_slot,
        coalesce(kc.children_in_no_class_section, 0) as children_in_no_class_section
    from chapters ch
    left join ay_status ays on ch.chapter_id = ays.chapter_id
    left join slot_counts sl on ch.chapter_id = sl.chapter_id
    left join circle_counts cc on ch.chapter_id = cc.chapter_id
    left join no_slot_circle_counts ns on ch.chapter_id = ns.chapter_id
    left join child_counts kc on ch.chapter_id = kc.chapter_id
)

select
    engine,
    chapter_id,
    chapter_name,
    city,
    co_name,
    cho_name,
    district,
    do_name,
    academic_year,
    total_slots,
    mentoring_circles_with_slot + mentoring_circles_no_slot as mentoring_circles_total,
    mentoring_circles_with_slot,
    mentoring_circles_no_slot,
    mentoring_circles_0_volunteers,
    mentoring_circles_1_volunteer,
    mentoring_circles_2plus_volunteers,
    (has_data_this_year
        and mentoring_circles_with_slot >= 1
        and mentoring_circles_no_slot = 0
        and mentoring_circles_0_volunteers = 0) as volunteer_requirement_met_1to5,
    -- A circle with no slot has no volunteer, so it fails 2:5 too.
    (has_data_this_year
        and mentoring_circles_with_slot >= 1
        and mentoring_circles_no_slot = 0
        and mentoring_circles_2plus_volunteers = mentoring_circles_with_slot) as volunteer_requirement_met_2to5,
    -- First reason that applies; null when met at 1:5.
    case
        when not has_data_this_year then 'no_data'
        when total_slots = 0 then 'no_slots'
        when mentoring_circles_with_slot = 0 then 'no_circles'
        when mentoring_circles_no_slot > 0 then 'circles_with_no_slot'
        when mentoring_circles_0_volunteers > 0 then 'circles_without_volunteer'
    end as volunteer_requirement_not_met_reason,
    mentoring_circles_started,
    mentoring_circles_not_started,
    (has_data_this_year
        and mentoring_circles_with_slot >= 1
        and mentoring_circles_not_started = 0) as sessions_commenced_met,
    -- First reason that applies; null when met.
    case
        when not has_data_this_year then 'no_data'
        when total_slots = 0 then 'no_slots'
        when mentoring_circles_with_slot = 0 then 'no_circles'
        when mentoring_circles_not_started > 0 then 'circles_not_started'
    end as sessions_commenced_not_met_reason,
    active_children,
    children_covered,
    children_in_circle_no_volunteer,
    children_in_circle_no_slot,
    children_in_no_class_section
from joined
