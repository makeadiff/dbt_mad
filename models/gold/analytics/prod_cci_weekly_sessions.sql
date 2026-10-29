{{ config(materialized='table') }}

-- CCI dashboard (E1): sessions held, one row per CCI per week (spec E1 §3.5, DO §3.3b shape). Counts
-- add up across weeks, so readers can multi-select weeks in Dalgo. Weeks start Monday; the first is
-- the week of 28 Sep, counting from 2026-10-01 only. The current week is labelled "(still filling)"
-- because volunteers log up to 7 days late.
--
-- Counted per mentoring circle: one circle-session is one circle on one date
-- (prod_cci_session_dates), however many volunteers were present. Planned and ideal sessions, and
-- % held, wait on the weekly-slot link (spec §7 ruling 1) and are not here.
--
-- Grain: one row per (chapter_id, week_start); every CCI with children x every week to date.

with params as (
    select (now() at time zone 'Asia/Kolkata')::date as today
),

weeks as (
    select
        w::date as week_start,
        'Week of ' || to_char(w, 'FMDD Mon')
            || case when w = date_trunc('week', p.today) then ' (still filling)' else '' end as week_label
    from params p
    cross join generate_series('2026-09-28'::date, date_trunc('week', p.today), interval '7 days') as w
),

per_week as (
    select
        c.chapter_id,
        date_trunc('week', d.session_date)::date as week_start,
        count(*) as sessions_held
    from {{ ref('prod_cci_mentoring_circles') }} c
    join {{ ref('prod_cci_session_dates') }} d
        on c.worknode_slot_shift_id = d.worknode_slot_shift_id
    where d.session_date >= date '2026-10-01'
    group by 1, 2
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.group_name,
    wk.week_start,
    wk.week_label,
    coalesce(pw.sessions_held, 0) as sessions_held
from {{ ref('prod_cci_chapter_summary') }} s
cross join weeks wk
left join per_week pw
    on s.chapter_id = pw.chapter_id
   and wk.week_start = pw.week_start
where s.has_children
