{{ config(materialized='table') }}

-- CCI dashboard (E1): sessions planned vs held, one row per CCI per week (spec E1 §3.5, layout v4
-- §7 L1 and L6, copying prod_do_weekly_sessions). Counts add up across weeks, so readers can
-- multi-select weeks in Dalgo. Weeks start Monday; the first is the week of 28 Sep, counting from
-- 2026-10-01 only. The current week is labelled "(still filling)" because volunteers log up to 7
-- days late. Each week also carries a month (the month holding the week's Thursday, so the week of
-- 28 Sep is October) for a month filter.
--
-- Counted per mentoring circle from prod_cci_planned_sessions: one circle-session is one circle with
-- a weekly slot day on one slot date, however many volunteers it has.
--   Planned: every circle with a slot day, one session a week from 2026-10-01 to today, whether or
--     not it has a volunteer, cancelled dates kept in.
--   Held: a planned circle-date with a volunteer or a substitute present; by a substitute when no
--     volunteer was present.
--   Cancelled: a cancellation logged and nothing held.
-- % columns are per row only; Dalgo charts them per week and never sums them.
--
-- Grain: one row per (chapter_id, week_start); every CCI with children x every week to date.

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

per_week as (
    select
        chapter_id,
        week_start,
        count(*) as sessions_planned,
        count(*) filter (where session_group in ('held_by_original', 'held_by_substitute')) as sessions_held,
        count(*) filter (where session_group = 'held_by_substitute') as sessions_held_by_substitute,
        count(*) filter (where session_group = 'cancelled') as sessions_cancelled
    from {{ ref('prod_cci_planned_sessions') }}
    group by 1, 2
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    wk.week_start,
    wk.week_label,
    wk.month_start,
    wk.month_label,
    coalesce(pw.sessions_planned, 0) as sessions_planned,
    coalesce(pw.sessions_held, 0) as sessions_held,
    coalesce(pw.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
    coalesce(pw.sessions_cancelled, 0) as sessions_cancelled,
    round(100.0 * pw.sessions_held / nullif(pw.sessions_planned, 0), 1) as pct_sessions_held,
    round(100.0 * pw.sessions_cancelled / nullif(pw.sessions_planned, 0), 1) as pct_sessions_cancelled
from {{ ref('prod_cci_chapter_summary') }} s
cross join weeks wk
left join per_week pw
    on s.chapter_id = pw.chapter_id
   and wk.week_start = pw.week_start
where s.has_children
