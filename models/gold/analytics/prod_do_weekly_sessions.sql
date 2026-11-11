{{ config(materialized='table') }}

-- DO dashboard (E2): sessions planned vs held, one row per chapter per week (spec §3.3b, planned
-- as amended by §3.8 ruling 2). Counts add up across weeks, so DOs can multi-select weeks in
-- Dalgo. Weeks start Monday; the first is the week of 28 Sep, counting from 2026-10-01 only. The
-- current week is labelled "(still filling)" because volunteers log up to 7 days late. Each week
-- also carries a month (the month holding the week's Thursday, so a week that straddles two months
-- goes to the one with most of its days; the week of 28 Sep is October) for a month filter.
--
-- Counted per mentoring circle, not per volunteer, from prod_do_circle_sessions: one circle-session
-- is one circle with a slot on one slot date, counted once however many volunteers it has.
-- Metric 3 stays per volunteer.
--   Planned: every circle with a slot, one session a week on its slot day from 2026-10-01 to
--     today, whether or not it has a volunteer, ignoring the slot-link start (what the dashboard
--     called "Ideal sessions" before 2026-11-11); cancelled dates dropped.
--   Held: a planned circle-date with a volunteer present or a substitute; by a substitute when no
--     volunteer was present.
-- The old E2 dashboard's "Ideal Session Count" (sections x 2 slots x weeks) is a different figure.
-- % held is per row only; Dalgo charts it per week and never sums it.
--
-- Grain: one row per (chapter_id, week_start); every chapter x every week to date.

with params as (
    select (now() at time zone 'Asia/Kolkata')::date as today
),

weeks as (
    select
        w::date as week_start,
        'Week of ' || to_char(w, 'FMDD Mon')
            || case when w = date_trunc('week', p.today) then ' (still filling)' else '' end as week_label,
        date_trunc('month', w + interval '3 days')::date as month_start,
        to_char(w + interval '3 days', 'FMMonth YYYY') as month_label
    from params p
    cross join generate_series('2026-09-28'::date, date_trunc('week', p.today), interval '7 days') as w
),

chapters as (
    select engine, chapter_id, chapter_name, city, co_name, cho_name, district, do_name
    from {{ ref('prod_do_chapter_summary') }}
),

per_week as (
    select
        chapter_id,
        week_start,
        count(*) as sessions_planned,
        count(*) filter (where session_group in ('held_by_original', 'held_by_substitute')) as sessions_held,
        count(*) filter (where session_group = 'held_by_substitute') as sessions_held_by_substitute
    from {{ ref('prod_do_circle_sessions') }}
    where session_group <> 'cancelled'
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
    wk.month_start,
    wk.month_label,
    coalesce(pw.sessions_planned, 0) as sessions_planned,
    coalesce(pw.sessions_held, 0) as sessions_held,
    coalesce(pw.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
    round(100.0 * pw.sessions_held / nullif(pw.sessions_planned, 0), 1) as pct_sessions_held
from weeks wk
cross join chapters ch
left join per_week pw
    on ch.chapter_id = pw.chapter_id
   and wk.week_start = pw.week_start
