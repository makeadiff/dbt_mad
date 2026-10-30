{{ config(materialized='table') }}

-- CCI dashboard (E1): one row per volunteer per CCI per period, without the planned and
-- consistency columns that wait on the slot link (spec §3.10). Sessions taken = the volunteer
-- marked present on a circle's session (int_pc_volunteer_attendance); hours contributed = sessions
-- taken x the slot's length (prod_cci_circle_sessions; 1 hour when a time is missing). A volunteer in
-- two circles has their sessions combined within the CCI. Only volunteers with >= 1 session taken
-- in the period get a row. Periods are in macros/session_periods.sql.
--
-- Grain: one row per (chapter_id, volunteer_id, period_type, period_start).

with periods as (
    {{ session_periods() }}
),

taken as (
    select distinct
        c.chapter_id,
        a.tagged_volunteer_id as volunteer_id,
        a.volunteer_name,
        a.section_slot_shift_id as worknode_slot_shift_id,
        a.scheduled_session_date::date as session_date
    from {{ ref('int_pc_volunteer_attendance') }} a
    join {{ ref('prod_cci_mentoring_circles') }} c
        on a.section_slot_shift_id = c.worknode_slot_shift_id
    where a.attendance_status = 'PRESENT'
      and a.tagged_volunteer_id is not null
      and a.scheduled_session_date::date >= date '2026-10-01'
),

names as (
    select volunteer_id, max(volunteer_name) as volunteer_name from taken group by 1
),

per_person as (
    select
        pr.period_type,
        pr.period_label,
        pr.period_sort,
        pr.period_start,
        pr.period_end,
        t.chapter_id,
        t.volunteer_id,
        count(*) as sessions_taken,
        sum(coalesce(cs.slot_hours, 1.0)) as hours_contributed
    from periods pr
    join taken t
        on t.session_date between pr.period_start and pr.period_end
    left join {{ ref('prod_cci_circle_sessions') }} cs
        on t.worknode_slot_shift_id = cs.worknode_slot_shift_id
       and t.session_date = cs.session_date
    group by 1, 2, 3, 4, 5, 6, 7
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    pp.period_type,
    pp.period_label,
    pp.period_sort,
    pp.period_start,
    pp.period_end,
    pp.volunteer_id,
    coalesce(n.volunteer_name, 'Volunteer ' || pp.volunteer_id) as volunteer_name,
    pp.sessions_taken,
    round(pp.hours_contributed::numeric, 1) as hours_contributed
from per_person pp
join {{ ref('prod_cci_chapter_summary') }} s
    on pp.chapter_id = s.chapter_id
left join names n
    on pp.volunteer_id = n.volunteer_id
