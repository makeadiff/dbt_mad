{{ config(materialized='table') }}

-- DO dashboard (E2): sessions planned vs held, one row per chapter per week (spec §3.3b). Counts
-- add up across weeks, so DOs can multi-select weeks in Dalgo. Weeks start Monday; the first is the
-- week of 28 Sep, counting from 2026-10-01 only. The current week is labelled "(still filling)"
-- because volunteers log up to 7 days late.
--
-- Counted per mentoring circle, not per volunteer: one circle-session is one circle with a slot
-- (prod_do_mentoring_circles) on one slot date, counted once however many volunteers it has.
-- Metric 3 stays per volunteer.
--   Ideal: every circle with a slot, one session a week on its slot day from 2026-10-01 to today,
--     cancelled dates dropped, ignoring the slot-link start.
--   Planned: the same, from the later of 2026-10-01 and the circle's slot-link start.
--   Held: a planned circle-date with a volunteer present or a substitute (same rule as
--     prod_do_mentoring_circles); by a substitute when no volunteer was present.
--   Cancelled: fct_e2_planned_session_status 'Cancelled' on one of the pair's rows, with nothing
--     held that date.
-- The old E2 dashboard's "Ideal Session Count" (sections x 2 slots x weeks) is a different figure.
-- % held is per row only; Dalgo charts it per week and never sums it.
--
-- Grain: one row per (chapter_id, week_start); every chapter x every week to date.

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

weeks as (
    select
        w::date as week_start,
        'Week of ' || to_char(w, 'FMDD Mon')
            || case when w = date_trunc('week', p.today) then ' (still filling)' else '' end as week_label
    from params p
    cross join generate_series('2026-09-28'::date, date_trunc('week', p.today), interval '7 days') as w
),

chapters as (
    select engine, chapter_id, chapter_name, city, co_name, cho_name, district, do_name
    from {{ ref('prod_do_chapter_summary') }}
),

circles as (
    select chapter_id, slot_id, class_section_id, slot_dow, slot_link_start_date
    from {{ ref('prod_do_mentoring_circles') }}
    where slot_dow is not null
),

circle_dates as (
    select c.chapter_id, c.slot_id, c.class_section_id, d::date as session_date, c.slot_link_start_date
    from circles c
    cross join params p
    cross join lateral generate_series(
        p.tracking_start + ((c.slot_dow - extract(dow from p.tracking_start)::int + 7) % 7),
        p.today,
        interval '7 days'
    ) as d
),

attendance as (
    select
        scs.slot_id,
        scs.class_section_id,
        a.date_of_slot::date as session_date,
        bool_or(upper(trim(a.attendance)) = 'TRUE') as has_present,
        bool_or(a.is_substitute) as has_substitute
    from {{ ref('fct_e2_volunteer_attendance_by_slot_date') }} a
    join {{ ref('int_bubble__slot_class_section') }} scs
        on a.slot_class_section_id = scs.slot_class_section_id
    where a.academic_year = '2026-2027'
    group by 1, 2, 3
),

cancelled_dates as (
    select distinct scs.slot_id, scs.class_section_id, p.planned_date as session_date
    from {{ ref('fct_e2_planned_session_status') }} p
    join {{ ref('int_bubble__slot_class_section') }} scs
        on p.slot_class_section_id = scs.slot_class_section_id
    where p.status = 'Cancelled'
      and scs.is_active and not scs.is_removed
),

flagged as (
    select
        cd.chapter_id,
        date_trunc('week', cd.session_date)::date as week_start,
        cd.session_date >= cd.slot_link_start_date as is_planned,
        coalesce(at.has_present or at.has_substitute, false) as is_held,
        coalesce(at.has_substitute and not at.has_present, false) as is_by_substitute
    from circle_dates cd
    left join attendance at
        on cd.slot_id = at.slot_id
       and cd.class_section_id = at.class_section_id
       and cd.session_date = at.session_date
    left join cancelled_dates cx
        on cd.slot_id = cx.slot_id
       and cd.class_section_id = cx.class_section_id
       and cd.session_date = cx.session_date
    where cx.slot_id is null
       or coalesce(at.has_present or at.has_substitute, false)
),

per_week as (
    select
        chapter_id,
        week_start,
        count(*) as ideal_sessions,
        count(*) filter (where is_planned) as sessions_planned,
        count(*) filter (where is_planned and is_held) as sessions_held,
        count(*) filter (where is_planned and is_by_substitute) as sessions_held_by_substitute
    from flagged
    group by 1, 2
)

select
    ch.engine,
    ch.chapter_id,
    ch.chapter_name,
    ch.city,
    ch.co_name,
    ch.cho_name,
    ch.district,
    ch.do_name,
    wk.week_start,
    wk.week_label,
    coalesce(pw.ideal_sessions, 0) as ideal_sessions,
    coalesce(pw.sessions_planned, 0) as sessions_planned,
    coalesce(pw.ideal_sessions, 0) - coalesce(pw.sessions_planned, 0) as sessions_lost_to_late_starts,
    coalesce(pw.sessions_held, 0) as sessions_held,
    coalesce(pw.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
    round(100.0 * pw.sessions_held / nullif(pw.sessions_planned, 0), 1) as pct_sessions_held
from weeks wk
cross join chapters ch
left join per_week pw
    on ch.chapter_id = pw.chapter_id
   and wk.week_start = pw.week_start
