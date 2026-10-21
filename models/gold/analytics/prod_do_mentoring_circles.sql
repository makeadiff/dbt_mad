{{ config(materialized='table') }}

-- DO dashboard (E2): every mentoring circle with a slot, one row per section-slot pair. Shared
-- base for prod_do_chapter_summary (Metrics 1 and 2) and prod_do_weekly_sessions (§3.3b), so the
-- circle rule lives in one place. Not read by Dalgo.
--
-- Reaches into silver (int_bubble__slot, int_bubble__slot_class_section,
-- int_bubble__slot_class_section_volunteer, int_bubble__class_section,
-- int_bubble__child_class_section) because no mart carries circle-grain data with the
-- active/removed flags at every level.
--
-- Live circle: one distinct (slot_id, class_section_id) pair whose slot_class_section row, slot
-- and class_section are all active and not removed, with the slot in AY 2026-27. The school_class
-- flag is deliberately NOT used: circles mix grades, so a live section can sit under a class
-- flagged removed (443's 8th) or have no class at all (554). A section with two slots is two
-- circles (554 = 18 sections, 26 circles). Pairs whose section has no active child are dropped
-- (383: 1). A volunteer on any of the pair's rows fills it.
--
-- Held (spec §3.2): a DOTS volunteer attendance record on any of the pair's rows (removed ones
-- included, as sessions are logged per row) marked present,
-- or absent with a substitute (fct_e2_volunteer_attendance_by_slot_date), in AY 2026-27.
--
-- Slot-link start: the earliest created_date of the pair's live slot_class_section rows. First
-- planned session: the first slot day on or after the link start that isn't cancelled
-- (fct_e2_planned_session_status 'Cancelled' on one of the pair's rows, with nothing held that
-- date). A circle is due once that date is before today.
--
-- Today is the IST date: the nightly job runs at 01:36 IST, when the UTC date is still yesterday.
--
-- Grain: one row per (slot_id, class_section_id).

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

school_years as (
    select say.school_academic_year_id, say.school_id::text as chapter_id
    from {{ ref('int_bubble__school_academic_year') }} say
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    where ay.label = '2026-2027'
),

live_slots as (
    select
        s.slot_id,
        sy.chapter_id,
        case upper(trim(s.day_of_week))
            when 'SUNDAY' then 0
            when 'MONDAY' then 1
            when 'TUESDAY' then 2
            when 'WEDNESDAY' then 3
            when 'THURSDAY' then 4
            when 'FRIDAY' then 5
            when 'SATURDAY' then 6
        end as slot_dow
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
    select scs.slot_class_section_id, scs.slot_id, scs.class_section_id, scs.created_date, ls.chapter_id, ls.slot_dow
    from {{ ref('int_bubble__slot_class_section') }} scs
    join live_slots ls
        on scs.slot_id = ls.slot_id
    join {{ ref('int_bubble__class_section') }} cs
        on scs.class_section_id = cs.class_section_id
    where scs.is_active and not scs.is_removed
      and cs.is_active and not cs.is_removed
),

-- Live sections at the chapter with >= 1 active child linked.
sections_with_children as (
    select distinct ls.chapter_id, ls.class_section_id
    from live_sections ls
    join {{ ref('int_bubble__child_class_section') }} ccs
        on ls.class_section_id = ccs.class_section_id
       and not ccs.is_removed
    join {{ ref('prod_do_active_children') }} ac
        on ccs.child_id = ac.child_id
       and ls.chapter_id = ac.chapter_id
),

circles as (
    select
        r.chapter_id,
        r.slot_id,
        r.class_section_id,
        min(r.slot_dow) as slot_dow,
        min(r.created_date)::date as slot_link_start_date,
        count(distinct v.volunteer_id) as volunteers
    from live_scs_rows r
    join sections_with_children swc
        on r.chapter_id = swc.chapter_id
       and r.class_section_id = swc.class_section_id
    left join {{ ref('int_bubble__slot_class_section_volunteer') }} v
        on r.slot_class_section_id = v.slot_class_section_id
       and v.is_active and not v.is_removed
    group by 1, 2, 3
),

held_dates as (
    select distinct scs.slot_id, scs.class_section_id, a.date_of_slot::date as session_date
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }} a
    join {{ ref('int_bubble__slot_class_section') }} scs
        on a.slot_class_section_id = scs.slot_class_section_id
    where a.academic_year = '2026-2027'
      and (upper(trim(a.attendance)) = 'TRUE' or a.is_substitute)
),

cancelled_dates as (
    select distinct r.slot_id, r.class_section_id, p.planned_date as session_date
    from {{ ref('fct_e2_planned_session_status') }} p
    join live_scs_rows r
        on p.slot_class_section_id = r.slot_class_section_id
    where p.status = 'Cancelled'
),

-- Slot days from the link start up to yesterday, cancelled ones dropped; the earliest is the first
-- planned session that has passed.
first_planned as (
    select c.slot_id, c.class_section_id, min(d::date) as first_planned_date
    from circles c
    cross join params p
    cross join lateral generate_series(
        c.slot_link_start_date + ((c.slot_dow - extract(dow from c.slot_link_start_date)::int + 7) % 7),
        p.today - 1,
        interval '7 days'
    ) as d
    where c.slot_dow is not null
      and not exists (
          select 1 from cancelled_dates cd
          where cd.slot_id = c.slot_id
            and cd.class_section_id = c.class_section_id
            and cd.session_date = d::date
            and not exists (
                select 1 from held_dates h
                where h.slot_id = c.slot_id
                  and h.class_section_id = c.class_section_id
                  and h.session_date = d::date
            )
      )
    group by 1, 2
),

held_summary as (
    select
        h.slot_id,
        h.class_section_id,
        bool_or(h.session_date < p.tracking_start) as held_before_1_oct,
        bool_or(h.session_date >= p.tracking_start) as held_since_1_oct
    from held_dates h
    cross join params p
    group by 1, 2
)

select
    c.chapter_id,
    c.slot_id,
    c.class_section_id,
    c.slot_dow,
    c.slot_link_start_date,
    c.volunteers,
    fp.first_planned_date,
    hs.slot_id is not null as is_started,
    coalesce(hs.held_before_1_oct, false) as held_before_1_oct,
    coalesce(hs.held_since_1_oct, false) as held_since_1_oct,
    -- A circle that has held a session counts as due, whatever its link start says.
    (hs.slot_id is not null or fp.first_planned_date is not null) as is_due
from circles c
left join first_planned fp
    on c.slot_id = fp.slot_id
   and c.class_section_id = fp.class_section_id
left join held_summary hs
    on c.slot_id = hs.slot_id
   and c.class_section_id = hs.class_section_id
