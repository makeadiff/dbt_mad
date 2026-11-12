{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model.
--
-- One-row national tiles for the DO dashboard, sourced from prod_do_national_kpis. No chapter
-- column by design (same reason as prod_sric_national_display): a Chapter filter then simply
-- doesn't apply to these tiles. Layout v4 (spec §3.8): setup complete, minimum volunteers, leads
-- (chapter links, city, open pool: shown together, never the chapter count alone) and planned
-- sessions are new; volunteer names changed; the 2:5 tiles are dropped.

select
    -- Header (spec §3.6b row 1), each % beside its count
    chapters_total                  as "Chapters",
    active_children                 as "Children",
    children_covered                as "Children with a volunteer",
    pct_children_covered            as "% of children with a volunteer",
    target_pct_children_covered     as "Target % of children with a volunteer",
    chapters_setup_complete         as "Chapters with setup complete",
    pct_chapters_setup_complete     as "% of chapters with setup complete",
    volunteers_minimum_to_start     as "Minimum volunteers to start sessions",
    volunteers_needed               as "Volunteers needed, one per mentoring circle",
    volunteers_in_chapter           as "Volunteers mapped to a chapter",
    volunteers_allocated            as "Volunteers assigned to a class section",
    pct_volunteers_placed           as "% of volunteers assigned to a class section",
    leads_chapter_link              as "Leads from chapter links",
    leads_city                      as "Leads in a city",
    leads_open_pool                 as "Leads open to any chapter",
    mentoring_circles_started       as "Mentoring circles started",
    pct_mentoring_circles_started   as "% of mentoring circles started",
    -- Goals and the rest
    chapters_met_1to5               as "Chapters where every mentoring circle has a volunteer",
    pct_chapters_met_1to5           as "% of chapters where every mentoring circle has a volunteer",
    target_pct_chapters_met         as "Target % of chapters",
    chapters_sessions_started       as "Chapters where sessions have started",
    pct_chapters_sessions_started   as "% of chapters where sessions have started",
    chapters_sessions_commenced     as "Chapters where every mentoring circle has started",
    pct_chapters_sessions_commenced as "% of chapters where every mentoring circle has started",
    target_pct_sessions_commenced   as "Target % of chapters where every mentoring circle has started",
    mentoring_circles_not_yet_due   as "Mentoring circles with first session still ahead",
    mentoring_circles_no_session_since_1_oct as "Mentoring circles with sessions before 1 October, none since",
    sessions_planned_since_1_oct    as "Sessions planned since 1 October",
    sessions_held_since_1_oct       as "Sessions held since 1 October",
    pct_sessions_held_since_1_oct   as "% of planned sessions held since 1 October",
    sessions_cancelled_since_1_oct  as "Sessions cancelled since 1 October",
    pct_sessions_cancelled_since_1_oct as "% of planned sessions cancelled since 1 October",
    volunteers_not_allocated        as "Volunteers mapped but not assigned to a class section",
    mentoring_circles_needing_volunteer as "Mentoring circles still needing a volunteer"
from {{ ref('prod_do_national_kpis') }}
