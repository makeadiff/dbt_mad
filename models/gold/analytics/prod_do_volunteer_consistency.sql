{{ config(materialized='table') }}

-- DO dashboard (E2): Metric 3, volunteer consistency, one row per chapter per period. Periods are
-- precomputed because Dalgo can't: "Since 1 October", each calendar month from October 2026, and
-- "Last 28 days" (clipped at 2026-10-01, the tracking start). Nothing before 1 October counts.
--
-- A volunteer is consistent when sessions taken / sessions planned >= 90% in the period, with their
-- sessions combined across circles within the chapter (one figure per volunteer per chapter). Planned
-- and taken come from prod_do_volunteer_session_dates. % consistent = consistent / volunteers with
-- >= 1 planned session; volunteers with a live allocation but no planned session in the period are
-- left out and counted alongside.
--
-- Grain: one row per (chapter_id, period_type, period_start); every chapter x every period.

with params as (
    select
        '2026-10-01'::date as tracking_start,
        (now() at time zone 'Asia/Kolkata')::date as today
),

periods as (
    select 'since_1_oct' as period_type, 'Since 1 October' as period_label, 1 as period_sort,
        p.tracking_start as period_start, p.today as period_end
    from params p
    union all
    select 'last_28_days', 'Last 28 days', 2,
        greatest(p.today - 27, p.tracking_start), p.today
    from params p
    union all
    select 'month', to_char(m, 'FMMonth YYYY'), 10 + row_number() over (order by m)::int,
        m::date, least((m + interval '1 month - 1 day')::date, p.today)
    from params p
    cross join generate_series(p.tracking_start, date_trunc('month', p.today), interval '1 month') as m
),

chapters as (
    select engine, chapter_id, chapter_name, city, co_name, cho_name, district, do_name
    from {{ ref('prod_do_chapter_summary') }}
),

per_volunteer as (
    select
        pr.period_type,
        pr.period_start,
        sd.chapter_id,
        sd.volunteer_id,
        count(*) as planned_sessions,
        count(*) filter (where sd.is_taken) as sessions_taken
    from {{ ref('prod_do_volunteer_session_dates') }} sd
    join periods pr
        on sd.session_date between pr.period_start and pr.period_end
    group by 1, 2, 3, 4
),

-- Volunteers with a live allocation at the chapter today, for the "no sessions planned yet" count.
allocated as (
    select distinct a.partner_id::text as chapter_id, a.volunteer_id
    from {{ ref('fct_e2_volunteer_allocation_history') }} a
    where a.academic_year = '2026-2027'
      and a.is_active
),

per_chapter as (
    select
        pr.period_type,
        pr.period_start,
        ch.chapter_id,
        count(pv.volunteer_id) as volunteers_with_planned_sessions,
        count(pv.volunteer_id) filter (where pv.sessions_taken >= 0.9 * pv.planned_sessions) as volunteers_consistent,
        coalesce(sum(pv.planned_sessions), 0) as planned_sessions,
        coalesce(sum(pv.sessions_taken), 0) as sessions_taken
    from periods pr
    cross join chapters ch
    left join per_volunteer pv
        on pr.period_type = pv.period_type
       and pr.period_start = pv.period_start
       and ch.chapter_id = pv.chapter_id
    group by 1, 2, 3
),

no_planned as (
    select pr.period_type, pr.period_start, al.chapter_id, count(*) as volunteers_no_sessions_planned
    from periods pr
    cross join allocated al
    where not exists (
        select 1 from per_volunteer pv
        where pv.period_type = pr.period_type
          and pv.period_start = pr.period_start
          and pv.chapter_id = al.chapter_id
          and pv.volunteer_id = al.volunteer_id
    )
    group by 1, 2, 3
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
    pr.period_type,
    pr.period_label,
    pr.period_sort,
    pr.period_start,
    pr.period_end,
    pc.volunteers_with_planned_sessions,
    coalesce(np.volunteers_no_sessions_planned, 0) as volunteers_no_sessions_planned,
    pc.volunteers_consistent,
    round(100.0 * pc.volunteers_consistent / nullif(pc.volunteers_with_planned_sessions, 0), 1) as pct_volunteers_consistent,
    70 as target_pct_volunteers_consistent,
    coalesce(100.0 * pc.volunteers_consistent / nullif(pc.volunteers_with_planned_sessions, 0) < 70, false) as below_target,
    pc.planned_sessions,
    pc.sessions_taken
from per_chapter pc
join periods pr
    on pc.period_type = pr.period_type
   and pc.period_start = pr.period_start
join chapters ch
    on pc.chapter_id = ch.chapter_id
left join no_planned np
    on pc.period_type = np.period_type
   and pc.period_start = np.period_start
   and pc.chapter_id = np.chapter_id
