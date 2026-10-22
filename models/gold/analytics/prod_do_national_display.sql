{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- One-row national tiles for the DO dashboard, sourced from prod_do_national_kpis. No chapter
-- column by design (same reason as prod_sric_national_display): a Chapter filter then simply
-- doesn't apply to these tiles.

select
    -- Header (spec §3.6b row 1), each % beside its count
    chapters_total                  as "Chapters",
    active_children                 as "Children",
    children_covered                as "Children with a volunteer",
    pct_children_covered            as "% of children with a volunteer",
    target_pct_children_covered     as "Target % of children with a volunteer",
    volunteers_needed               as "Volunteers needed (1:5)",
    volunteers_needed_2to5          as "Volunteers needed (2:5)",
    volunteers_in_chapter           as "Volunteers signed up",
    volunteers_allocated            as "Volunteers placed in a mentoring circle",
    pct_volunteers_placed           as "% of volunteers placed",
    mentoring_circles_started       as "Mentoring circles started",
    pct_mentoring_circles_started   as "% of mentoring circles started",
    -- Goals and the rest
    chapters_met_1to5               as "Chapters where every mentoring circle has a volunteer",
    pct_chapters_met_1to5           as "% of chapters where every mentoring circle has a volunteer",
    chapters_met_2to5               as "Chapters where every mentoring circle has 2 volunteers",
    pct_chapters_met_2to5           as "% of chapters where every mentoring circle has 2 volunteers",
    target_pct_chapters_met         as "Target % of chapters",
    chapters_sessions_started       as "Chapters where sessions have started",
    pct_chapters_sessions_started   as "% of chapters where sessions have started",
    chapters_sessions_commenced     as "Chapters where every mentoring circle has started",
    pct_chapters_sessions_commenced as "% of chapters where every mentoring circle has started",
    target_pct_sessions_commenced   as "Target % of chapters where every mentoring circle has started",
    mentoring_circles_not_yet_due   as "Mentoring circles with first session still ahead",
    mentoring_circles_no_session_since_1_oct as "Mentoring circles with sessions in September, none since 1 Oct",
    sessions_held_since_1_oct       as "Sessions held since 1 October",
    volunteers_not_allocated        as "Volunteers signed up but not placed",
    mentoring_circles_needing_volunteer as "Mentoring circles still needing a volunteer"
from {{ ref('prod_do_national_kpis') }}
