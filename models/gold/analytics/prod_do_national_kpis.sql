{{ config(materialized='table') }}

-- DO dashboard (E2): one-row national headline for Metrics 1 and 2 and child cover. Every % is a ratio
-- of sums across prod_do_chapter_summary's 62 chapters, not an average of per-chapter %. Chapters
-- not met stay in the denominator. Volunteer counts are sums across chapters (a volunteer sits in
-- one chapter, their most recent, so none count twice on 2026-10-22). Sessions held since 1 October sums
-- prod_do_weekly_sessions (circle-sessions, spec §3.3b).
--
-- Grain: exactly one row (engine = 'E2').

with totals as (
    select
        count(*) as chapters_total,
        count(*) filter (where volunteer_requirement_met_1to5) as chapters_met_1to5,
        count(*) filter (where volunteer_requirement_met_2to5) as chapters_met_2to5,
        count(*) filter (where has_sessions_started) as chapters_sessions_started,
        count(*) filter (where sessions_commenced_met) as chapters_sessions_commenced,
        sum(mentoring_circles_with_slot)::bigint as mentoring_circles_with_slot,
        sum(mentoring_circles_not_yet_due)::bigint as mentoring_circles_not_yet_due,
        sum(mentoring_circles_started)::bigint as mentoring_circles_started,
        sum(mentoring_circles_not_started)::bigint as mentoring_circles_not_started,
        sum(mentoring_circles_no_session_since_1_oct)::bigint as mentoring_circles_no_session_since_1_oct,
        sum(volunteers_needed)::bigint as volunteers_needed,
        sum(volunteers_in_chapter)::bigint as volunteers_in_chapter,
        sum(volunteers_allocated)::bigint as volunteers_allocated,
        sum(volunteers_not_allocated)::bigint as volunteers_not_allocated,
        sum(mentoring_circles_needing_volunteer)::bigint as mentoring_circles_needing_volunteer,
        sum(active_children)::bigint as active_children,
        sum(children_covered)::bigint as children_covered
    from {{ ref('prod_do_chapter_summary') }}
),

sessions as (
    select sum(sessions_held)::bigint as sessions_held_since_1_oct
    from {{ ref('prod_do_weekly_sessions') }}
)

select
    'E2' as engine,
    chapters_total,
    chapters_met_1to5,
    round(100.0 * chapters_met_1to5 / nullif(chapters_total, 0), 1) as pct_chapters_met_1to5,
    chapters_met_2to5,
    round(100.0 * chapters_met_2to5 / nullif(chapters_total, 0), 1) as pct_chapters_met_2to5,
    80 as target_pct_chapters_met,
    chapters_sessions_started,
    round(100.0 * chapters_sessions_started / nullif(chapters_total, 0), 1) as pct_chapters_sessions_started,
    chapters_sessions_commenced,
    round(100.0 * chapters_sessions_commenced / nullif(chapters_total, 0), 1) as pct_chapters_sessions_commenced,
    80 as target_pct_sessions_commenced,
    mentoring_circles_with_slot,
    mentoring_circles_not_yet_due,
    mentoring_circles_started,
    mentoring_circles_not_started,
    -- Of circles that are due (not-yet-due circles are left out).
    round(100.0 * mentoring_circles_started / nullif(mentoring_circles_started + mentoring_circles_not_started, 0), 1) as pct_mentoring_circles_started,
    mentoring_circles_no_session_since_1_oct,
    sessions_held_since_1_oct,
    volunteers_needed,
    volunteers_in_chapter,
    volunteers_allocated,
    volunteers_not_allocated,
    mentoring_circles_needing_volunteer,
    active_children,
    children_covered,
    round(100.0 * children_covered / nullif(active_children, 0), 1) as pct_children_covered,
    100 as target_pct_children_covered
from totals
cross join sessions
