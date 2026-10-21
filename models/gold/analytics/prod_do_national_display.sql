{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- One-row national tiles for the DO dashboard, sourced from prod_do_national_kpis. No chapter
-- column by design (same reason as prod_sric_national_display): a Chapter filter then simply
-- doesn't apply to these tiles.

select
    chapters_total                  as "Chapters",
    chapters_met_1to5               as "Chapters meeting volunteer requirement (1:5)",
    pct_chapters_met_1to5           as "% of chapters meeting volunteer requirement (1:5)",
    chapters_met_2to5               as "Chapters meeting volunteer requirement (2:5)",
    pct_chapters_met_2to5           as "% of chapters meeting volunteer requirement (2:5)",
    target_pct_chapters_met         as "Target % of chapters",
    chapters_sessions_started       as "Chapters with sessions started",
    pct_chapters_sessions_started   as "% of chapters with sessions started",
    chapters_sessions_commenced     as "Chapters with all mentoring circles started",
    pct_chapters_sessions_commenced as "% of chapters with all mentoring circles started",
    target_pct_sessions_commenced   as "Target % of chapters with all mentoring circles started",
    pct_mentoring_circles_started   as "% of mentoring circles started",
    mentoring_circles_not_yet_due   as "Mentoring circles with first session upcoming",
    mentoring_circles_no_session_since_1_oct as "Mentoring circles with sessions in September, none since 1 Oct",
    sessions_held_since_1_oct       as "Sessions held since 1 October",
    volunteers_needed               as "Volunteers needed",
    volunteers_in_chapter           as "Volunteers in the chapter",
    volunteers_allocated            as "Volunteers allocated to a mentoring circle",
    volunteers_not_allocated        as "Volunteers not allocated",
    mentoring_circles_needing_volunteer as "Mentoring circles still needing a volunteer",
    active_children                 as "Active children",
    children_covered                as "Children covered",
    pct_children_covered            as "% of children covered",
    target_pct_children_covered     as "Target % of children covered"
from {{ ref('prod_do_national_kpis') }}
