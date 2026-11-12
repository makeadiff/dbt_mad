{{ config(materialized='table') }}

-- DO dashboard (E2): one-row national headline for Metrics 1 and 2 and child cover. Every % is a ratio
-- of sums across prod_do_chapter_summary's 62 chapters, not an average of per-chapter %. Chapters
-- not met stay in the denominator. Volunteer counts are sums across chapters (a volunteer sits in
-- one chapter, their most recent, so none count twice on 2026-10-22). Sessions held since 1 October sums
-- prod_do_weekly_sessions (circle-sessions, spec §3.3b, planned per §3.8 ruling 2).
-- Layout v4 (spec §3.8): chapters with setup complete and %, minimum volunteers to start (sum of
-- each chapter's ceiling), and leads from fct_volunteer_pipeline (applied, not hired, 26-27
-- cohort): from chapter links (summed over the 62 chapters, as the district rollup), in a city
-- (every 'city' lead, whether or not an E2 chapter is in that city) and open to any chapter
-- (open_pool). Worknode-scoped leads with no campaign match (chapter_unmatched) are left out of
-- all three, per SRI spec §12.
--
-- Metric 4, baseline complete (spec §3.4): summed from prod_do_baseline_chapters, target 80% of
-- active children, ratio of sums.
--
-- Grain: exactly one row (engine = 'E2').

with totals as (
    select
        count(*) as chapters_total,
        count(*) filter (where volunteer_requirement_met_1to5) as chapters_met_1to5,
        count(*) filter (where volunteer_requirement_met_2to5) as chapters_met_2to5,
        count(*) filter (where has_sessions_started) as chapters_sessions_started,
        count(*) filter (where sessions_commenced_met) as chapters_sessions_commenced,
        count(*) filter (where setup_complete) as chapters_setup_complete,
        sum(volunteers_minimum_to_start)::bigint as volunteers_minimum_to_start,
        sum(leads_chapter_link)::bigint as leads_chapter_link,
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
    select
        sum(sessions_planned)::bigint as sessions_planned_since_1_oct,
        sum(sessions_held)::bigint as sessions_held_since_1_oct,
        sum(sessions_cancelled)::bigint as sessions_cancelled_since_1_oct
    from {{ ref('prod_do_weekly_sessions') }}
),

leads as (
    select
        count(distinct volunteer_id) filter (where lead_attribution = 'city') as leads_city,
        count(distinct volunteer_id) filter (where lead_attribution = 'open_pool') as leads_open_pool
    from {{ ref('fct_volunteer_pipeline') }}
    where is_applied and not is_recruited_new
),

baseline as (
    select
        sum(children_numeracy_done)::bigint as children_numeracy_done,
        sum(children_literacy_done)::bigint as children_literacy_done,
        sum(children_life_skills_done)::bigint as children_life_skills_done,
        sum(children_0_forms)::bigint as children_0_forms,
        sum(children_1_form)::bigint as children_1_form,
        sum(children_2_forms)::bigint as children_2_forms,
        sum(children_3_forms)::bigint as children_3_forms
    from {{ ref('prod_do_baseline_chapters') }}
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
    mentoring_circles_started + mentoring_circles_not_started as mentoring_circles_due,
    -- Of circles that are due (not-yet-due circles are left out).
    round(100.0 * mentoring_circles_started / nullif(mentoring_circles_started + mentoring_circles_not_started, 0), 1) as pct_mentoring_circles_started,
    mentoring_circles_no_session_since_1_oct,
    sessions_planned_since_1_oct,
    sessions_held_since_1_oct,
    round(100.0 * sessions_held_since_1_oct / nullif(sessions_planned_since_1_oct, 0), 1) as pct_sessions_held_since_1_oct,
    sessions_cancelled_since_1_oct,
    round(100.0 * sessions_cancelled_since_1_oct / nullif(sessions_planned_since_1_oct, 0), 1) as pct_sessions_cancelled_since_1_oct,
    chapters_setup_complete,
    round(100.0 * chapters_setup_complete / nullif(chapters_total, 0), 1) as pct_chapters_setup_complete,
    volunteers_minimum_to_start,
    volunteers_needed,
    -- 2 per mentoring circle, the 2:5 requirement (spec §3.6b).
    2 * volunteers_needed as volunteers_needed_2to5,
    volunteers_in_chapter,
    volunteers_allocated,
    round(100.0 * volunteers_allocated / nullif(volunteers_in_chapter, 0), 1) as pct_volunteers_placed,
    volunteers_not_allocated,
    mentoring_circles_needing_volunteer,
    active_children,
    children_covered,
    round(100.0 * children_covered / nullif(active_children, 0), 1) as pct_children_covered,
    100 as target_pct_children_covered,
    leads_chapter_link,
    leads_city,
    leads_open_pool,
    children_numeracy_done,
    children_literacy_done,
    children_life_skills_done,
    children_0_forms,
    children_1_form,
    children_2_forms,
    children_3_forms,
    children_3_forms as children_baseline_complete,
    round(100.0 * children_3_forms / nullif(active_children, 0), 1) as pct_baseline_complete,
    80 as target_pct_baseline_complete
from totals
cross join sessions
cross join leads
cross join baseline
