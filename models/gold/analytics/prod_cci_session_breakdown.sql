{{ config(materialized='table') }}

-- CCI dashboard (E1): sessions held by the original volunteer vs a substitute, and sessions
-- cancelled, one row per CCI per week (spec §3.10). There is no planned count until the slot link
-- lands (§7 ruling 1), so no "no attendance logged" or "volunteer absent" groups and no % of
-- planned; the % is of sessions held. Weeks as prod_cci_weekly_sessions: Monday start, from the
-- week of 28 Sep, the current one "(still filling)".
--
-- Grain: one row per (chapter_id, week_start); every CCI with children x every week to date.

with weeks as (
    select week_start, week_label from {{ ref('prod_cci_weekly_sessions') }} group by 1, 2
),

held as (
    select
        chapter_id,
        week_start,
        count(*) filter (where held_by_original) as sessions_held_by_original,
        count(*) filter (where held_by_substitute) as sessions_held_by_substitute
    from {{ ref('prod_cci_circle_sessions') }}
    where session_date >= date '2026-10-01'
    group by 1, 2
),

cancelled as (
    select chapter_id, week_start, count(*) as sessions_cancelled
    from {{ ref('prod_cci_cancellations') }}
    group by 1, 2
),

joined as (
    select
        s.chapter_id,
        s.chapter_name,
        s.city,
        s.district,
        s.do_name,
        wk.week_start,
        wk.week_label,
        coalesce(h.sessions_held_by_original, 0) as sessions_held_by_original,
        coalesce(h.sessions_held_by_substitute, 0) as sessions_held_by_substitute,
        coalesce(c.sessions_cancelled, 0) as sessions_cancelled
    from {{ ref('prod_cci_chapter_summary') }} s
    cross join weeks wk
    left join held h
        on s.chapter_id = h.chapter_id
       and wk.week_start = h.week_start
    left join cancelled c
        on s.chapter_id = c.chapter_id
       and wk.week_start = c.week_start
    where s.has_children
)

select
    j.*,
    j.sessions_held_by_original + j.sessions_held_by_substitute as sessions_held,
    round(100.0 * j.sessions_held_by_original
        / nullif(j.sessions_held_by_original + j.sessions_held_by_substitute, 0), 1) as pct_held_by_original,
    round(100.0 * j.sessions_held_by_substitute
        / nullif(j.sessions_held_by_original + j.sessions_held_by_substitute, 0), 1) as pct_held_by_substitute
from joined j
