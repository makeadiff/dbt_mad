{{ config(materialized='table') }}

-- DO dashboard (E2): one row per district plus an "All districts" row (spec §3.6b row 2), because
-- Dalgo can't total a table. Rolled up from prod_do_chapter_summary; every % is a ratio of sums.
-- One DO per district (seed_do_chapter_district), so do_name rides along; the All districts row
-- has none. district_sort puts the districts in number order and All districts last (8), since
-- "All districts" would sort first by name. Baseline columns come with Metric 4.
--
-- Sessions planned / held are circle-sessions from 1 Oct to today (spec §3.3b, §3.8 ruling 2),
-- summed from prod_do_weekly_sessions; planned includes cancelled dates; not held = planned - held.
-- Layout v4 (spec §3.8): chapters with setup complete and its %, minimum volunteers to start
-- (sum of each chapter's ceiling) and leads from chapter links (additive; city and open-pool leads
-- aren't per chapter, so they live on prod_do_national_kpis only).
--
-- Grain: one row per district, plus one 'All districts' row.

with chapter_sessions as (
    select
        chapter_id,
        sum(sessions_planned)::bigint as sessions_planned,
        sum(sessions_held)::bigint as sessions_held,
        sum(sessions_cancelled)::bigint as sessions_cancelled
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1
),

chapters as (
    select s.*, coalesce(w.sessions_planned, 0) as sessions_planned, coalesce(w.sessions_held, 0) as sessions_held,
        coalesce(w.sessions_cancelled, 0) as sessions_cancelled
    from {{ ref('prod_do_chapter_summary') }} s
    left join chapter_sessions w on s.chapter_id = w.chapter_id
),

rolled as (
    select
        district,
        do_name,
        substring(district from '[0-9]+')::int as district_sort,
        count(*) as chapters,
        sum(active_children)::bigint as active_children,
        sum(children_covered)::bigint as children_covered,
        count(*) filter (where total_slots = 0) as chapters_no_slots,
        count(*) filter (where setup_complete) as chapters_setup_complete,
        sum(volunteers_minimum_to_start)::bigint as volunteers_minimum_to_start,
        sum(volunteers_needed)::bigint as volunteers_needed,
        sum(leads_chapter_link)::bigint as leads_chapter_link,
        sum(volunteers_in_chapter)::bigint as volunteers_in_chapter,
        sum(volunteers_allocated)::bigint as volunteers_allocated,
        count(*) filter (where volunteer_requirement_met_1to5) as chapters_met_1to5,
        sum(mentoring_circles_started)::bigint as mentoring_circles_started,
        sum(mentoring_circles_started + mentoring_circles_not_started)::bigint as mentoring_circles_due,
        sum(sessions_planned)::bigint as sessions_planned,
        sum(sessions_held)::bigint as sessions_held,
        sum(sessions_cancelled)::bigint as sessions_cancelled
    from chapters
    group by 1, 2

    union all

    select
        'All districts',
        null,
        8,
        count(*),
        sum(active_children)::bigint,
        sum(children_covered)::bigint,
        count(*) filter (where total_slots = 0),
        count(*) filter (where setup_complete),
        sum(volunteers_minimum_to_start)::bigint,
        sum(volunteers_needed)::bigint,
        sum(leads_chapter_link)::bigint,
        sum(volunteers_in_chapter)::bigint,
        sum(volunteers_allocated)::bigint,
        count(*) filter (where volunteer_requirement_met_1to5),
        sum(mentoring_circles_started)::bigint,
        sum(mentoring_circles_started + mentoring_circles_not_started)::bigint,
        sum(sessions_planned)::bigint,
        sum(sessions_held)::bigint,
        sum(sessions_cancelled)::bigint
    from chapters
)

select
    district,
    do_name,
    district_sort,
    chapters,
    active_children,
    children_covered,
    round(100.0 * children_covered / nullif(active_children, 0), 1) as pct_children_covered,
    chapters_no_slots,
    chapters_setup_complete,
    round(100.0 * chapters_setup_complete / nullif(chapters, 0), 1) as pct_chapters_setup_complete,
    volunteers_minimum_to_start,
    volunteers_needed,
    leads_chapter_link,
    volunteers_in_chapter,
    volunteers_allocated,
    round(100.0 * volunteers_allocated / nullif(volunteers_in_chapter, 0), 1) as pct_volunteers_placed,
    chapters_met_1to5,
    round(100.0 * chapters_met_1to5 / nullif(chapters, 0), 1) as pct_chapters_met_1to5,
    mentoring_circles_started,
    mentoring_circles_due,
    round(100.0 * mentoring_circles_started / nullif(mentoring_circles_due, 0), 1) as pct_mentoring_circles_started,
    sessions_planned,
    sessions_held,
    sessions_planned - sessions_held as sessions_not_held,
    sessions_cancelled,
    round(100.0 * sessions_held / nullif(sessions_planned, 0), 1) as pct_sessions_held,
    round(100.0 * sessions_cancelled / nullif(sessions_planned, 0), 1) as pct_sessions_cancelled
from rolled
