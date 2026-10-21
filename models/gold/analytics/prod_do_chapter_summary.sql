{{ config(materialized='table') }}

-- DO dashboard (E2): one row per active E2 chapter for AY 2026-27. Metric 1 (volunteer
-- requirement met, 1:5 and 2:5), Metric 2 (sessions commenced) and child cover. Metric 4 joins
-- this model later.
--
-- Circles with a slot come from prod_do_mentoring_circles (one per section-slot pair, see its
-- header) and active children from prod_do_active_children. Reaches into silver
-- (int_bubble__slot, int_bubble__class_section, int_bubble__child_class_section) for the slot
-- count, the no-slot circles and child cover. A mentoring circle is a live class section with
-- active children; mentoring_circles_total adds circles with a slot and circles with no slot.
--
-- Mentoring circle with no slot: a live class section (active, not removed, in the chapter's
-- 2026-27 year) with >= 1 active child but no live slot link, one per section. In Bubble a section
-- only gets a slot link once a volunteer is placed, so these are the real volunteer gap (604: 4).
-- Sections with no active children are ignored.
--
-- Child cover: a child's sections are their not-removed child_class_section links to live
-- sections at the chapter, with no match to the child's enrolled class. Each child lands in the
-- worst group that applies: in no class section > in a circle with no slot > in a circle with no
-- volunteer > covered. The four groups sum to active_children.
--
-- Metric 2 (spec §3.2): a circle with a slot has started when >= 1 session was held in AY
-- 2026-27 (present, or absent with a substitute). Planned or cancelled sessions don't count.
-- Circles with no slot, and circles not yet due (first planned session still ahead), are left out
-- and shown alongside; a chapter needs >= 1 due circle to meet Metric 2. Circles that started before 1 Oct but have held nothing since are flagged
-- (mentoring_circles_no_session_since_1_oct); the flag doesn't change the met flag.
--
-- District and DO come from seed_do_chapter_district (a temporary seed from the DO dashboard
-- sheet, until district lands in a mapping source).
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

circles as (
    select * from {{ ref('prod_do_mentoring_circles') }}
),

-- Per live section: does it have a slot link, and does any of its circles lack a volunteer.
-- Sections with no active child have no circles here, but no active child sits in them either.
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

active_children as (
    select chapter_id, child_id
    from {{ ref('prod_do_active_children') }}
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

circle_counts as (
    select
        chapter_id,
        count(*) as mentoring_circles_with_slot,
        count(*) filter (where volunteers = 0) as mentoring_circles_0_volunteers,
        count(*) filter (where volunteers = 1) as mentoring_circles_1_volunteer,
        count(*) filter (where volunteers >= 2) as mentoring_circles_2plus_volunteers,
        count(*) filter (where not is_due) as mentoring_circles_not_yet_due,
        count(*) filter (where is_due and is_started) as mentoring_circles_started,
        count(*) filter (where is_due and not is_started) as mentoring_circles_not_started,
        count(*) filter (where held_before_1_oct and not held_since_1_oct) as mentoring_circles_no_session_since_1_oct
    from circles
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
        dd.district,
        dd.do_name,
        '2026-2027' as academic_year,
        coalesce(ays.is_ay_active, false) as has_data_this_year,
        coalesce(sl.total_slots, 0) as total_slots,
        coalesce(cc.mentoring_circles_with_slot, 0) as mentoring_circles_with_slot,
        coalesce(ns.mentoring_circles_no_slot, 0) as mentoring_circles_no_slot,
        coalesce(cc.mentoring_circles_0_volunteers, 0) as mentoring_circles_0_volunteers,
        coalesce(cc.mentoring_circles_1_volunteer, 0) as mentoring_circles_1_volunteer,
        coalesce(cc.mentoring_circles_2plus_volunteers, 0) as mentoring_circles_2plus_volunteers,
        coalesce(cc.mentoring_circles_not_yet_due, 0) as mentoring_circles_not_yet_due,
        coalesce(cc.mentoring_circles_started, 0) as mentoring_circles_started,
        coalesce(cc.mentoring_circles_not_started, 0) as mentoring_circles_not_started,
        coalesce(cc.mentoring_circles_no_session_since_1_oct, 0) as mentoring_circles_no_session_since_1_oct,
        coalesce(kc.active_children, 0) as active_children,
        coalesce(kc.children_covered, 0) as children_covered,
        coalesce(kc.children_in_circle_no_volunteer, 0) as children_in_circle_no_volunteer,
        coalesce(kc.children_in_circle_no_slot, 0) as children_in_circle_no_slot,
        coalesce(kc.children_in_no_class_section, 0) as children_in_no_class_section
    from chapters ch
    left join {{ ref('seed_do_chapter_district') }} dd on ch.chapter_id = dd.chapter_id
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
    mentoring_circles_not_yet_due,
    mentoring_circles_started,
    mentoring_circles_not_started,
    mentoring_circles_no_session_since_1_oct,
    -- Of circles that are due (not-yet-due circles are left out).
    round(100.0 * mentoring_circles_started / nullif(mentoring_circles_started + mentoring_circles_not_started, 0), 1) as pct_mentoring_circles_started,
    -- Needs >= 1 due circle: a chapter whose circles are all not yet due hasn't commenced (600).
    (has_data_this_year
        and mentoring_circles_started >= 1
        and mentoring_circles_not_started = 0) as sessions_commenced_met,
    -- First reason that applies; null when met.
    case
        when not has_data_this_year then 'no_data'
        when total_slots = 0 then 'no_slots'
        when mentoring_circles_with_slot = 0 then 'no_circles'
        when mentoring_circles_not_started > 0 then 'circles_not_started'
        when mentoring_circles_started = 0 then 'circles_not_yet_due'
    end as sessions_commenced_not_met_reason,
    active_children,
    children_covered,
    children_in_circle_no_volunteer,
    children_in_circle_no_slot,
    children_in_no_class_section,
    round(100.0 * children_covered / nullif(active_children, 0), 1) as pct_children_covered
from joined
