{{ config(materialized='table') }}

-- DO dashboard (E2): one row per volunteer per chapter per week (spec §3.9 W1), so volunteer
-- attendance follows the Dalgo Week and Month filters. Same planned and taken rules as
-- prod_do_volunteer_sessions (Metric 3, prod_do_volunteer_session_dates); only the time grain
-- changes. Weeks and months come from prod_do_weekly_sessions (Monday start, the week of 28 Sep
-- first, the current week "(still filling)", the month holding the week's Thursday), the same
-- source as prod_do_session_breakdown, so Week / Week starting / Month / Month starting match
-- prod_do_session_breakdown_display exactly. Sessions count from 2026-10-01, so the weeks of a
-- person add up to their "Since 1 October" row in prod_do_volunteer_sessions (test). Zero-planned
-- weeks get no row. No % or Status column: Dalgo sums taken and planned over the weeks picked and
-- computes them there (W2, W3).
--
-- Grain: one row per (chapter_id, volunteer_id, week_start).

with weeks as (
    select
        week_start,
        week_label,
        month_start,
        month_label,
        greatest(week_start, '2026-10-01'::date) as range_start,
        least(week_start + 6, (now() at time zone 'Asia/Kolkata')::date) as range_end
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1, 2, 3, 4
),

names as (
    select volunteer_id, max(nullif(trim(user_display_name), '')) as volunteer_name
    from {{ ref('int_bubble__user') }}
    group by 1
),

per_person as (
    select
        wk.week_start,
        wk.week_label,
        wk.month_start,
        wk.month_label,
        sd.chapter_id,
        sd.volunteer_id,
        count(*) as sessions_planned,
        count(*) filter (where sd.is_taken) as sessions_taken
    from weeks wk
    join {{ ref('prod_do_volunteer_session_dates') }} sd
        on sd.session_date between wk.range_start and wk.range_end
    group by 1, 2, 3, 4, 5, 6
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
    pp.week_start,
    pp.week_label,
    pp.month_start,
    pp.month_label,
    pp.volunteer_id,
    coalesce(n.volunteer_name, 'Volunteer ' || pp.volunteer_id) as volunteer_name,
    pp.sessions_planned,
    pp.sessions_taken
from per_person pp
join {{ ref('prod_do_chapter_summary') }} ch
    on pp.chapter_id = ch.chapter_id
left join names n
    on pp.volunteer_id = n.volunteer_id
