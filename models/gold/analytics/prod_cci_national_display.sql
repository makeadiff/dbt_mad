{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One-row national tiles for the CCI dashboard (spec E1 §7 L4, L7,
-- following prod_do_national_display): the All districts row of prod_cci_district_summary, plus the
-- leads that aren't per CCI. No chapter column by design, so a Chapter filter doesn't apply here.
--
-- Leads (applied, not hired, 2026-27 cohort, fct_volunteer_pipeline): from CCI links (summed over
-- the CCIs, prod_cci_chapter_summary's rule), in a city (every 'city' lead in a CCI city, less those
-- who applied through a CCI link, Gurugram counted for Delhi), and open to any chapter (open_pool,
-- the same national figure as School's). Shown side by side, never the CCI-link count alone.

with all_row as (
    select * from {{ ref('prod_cci_district_summary') }}
    where district = 'All districts'
),

cci_cities as (
    select distinct lower(trim(city)) as city_key
    from {{ ref('prod_cci_chapter_summary') }}
),

leads as (
    select
        count(distinct f.volunteer_id) filter (
            where f.lead_attribution = 'city'
              and (case lower(trim(f.city)) when 'gurugram' then 'delhi' else lower(trim(f.city)) end)
                  in (select city_key from cci_cities)
              and not exists (select 1 from {{ ref('prod_cci_link_applicants') }} la where la.volunteer_id = f.volunteer_id)
        ) as leads_city_all,
        count(distinct f.volunteer_id) filter (where f.lead_attribution = 'open_pool') as leads_open_pool
    from {{ ref('fct_volunteer_pipeline') }} f
    where f.is_applied and not f.is_recruited_new
)

select
    a.ccis                                      as "CCIs",
    a.active_children                           as "Children",
    a.children_with_volunteer                   as "Children with a volunteer",
    a.pct_children_with_volunteer               as "% of children with a volunteer",
    a.ccis_setup_complete                       as "CCIs with setup complete",
    a.pct_ccis_setup_complete                   as "% of CCIs with setup complete",
    a.weekly_session_slots                      as "Weekly session slots",
    a.mentoring_circles                         as "Mentoring circles",
    a.children_not_in_circle                    as "Children not yet in a mentoring circle",
    a.volunteers_minimum_to_start               as "Minimum volunteers to start sessions",
    a.volunteers_needed_as_set_up_1to5          as "Volunteers needed, one per mentoring circle",
    a.volunteers_placed                         as "Volunteers assigned to a class section",
    a.leads_chapter_link                        as "Leads from chapter links",
    l.leads_city_all                            as "Leads in a city",
    l.leads_open_pool                           as "Leads open to any chapter",
    a.circles_needing_volunteer                 as "Mentoring circles still needing a volunteer",
    a.circles_no_weekly_slot                    as "Mentoring circles with no weekly slot",
    a.circles_started                           as "Mentoring circles started",
    a.pct_circles_started                       as "% of mentoring circles started",
    a.sessions_planned_since_1_oct              as "Sessions planned since 1 October",
    a.sessions_held_by_original_since_1_oct     as "Sessions held by the original volunteer since 1 October",
    a.pct_held_by_original_since_1_oct          as "% of planned sessions held by the original volunteer",
    a.sessions_held_by_substitute_since_1_oct   as "Sessions held by a substitute since 1 October",
    a.sessions_held_since_1_oct                 as "Sessions held since 1 October",
    a.pct_sessions_held_since_1_oct             as "% of planned sessions held since 1 October",
    a.sessions_cancelled_since_1_oct            as "Sessions cancelled since 1 October",
    a.pct_sessions_cancelled_since_1_oct        as "% of planned sessions cancelled since 1 October",
    a.sessions_volunteer_absent_since_1_oct     as "Volunteer absent since 1 October",
    a.sessions_no_volunteer_assigned_since_1_oct as "No volunteer assigned since 1 October"
from all_row a
cross join leads l
