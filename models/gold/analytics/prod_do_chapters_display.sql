{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter, sourced from prod_do_chapter_summary; the
-- not-met reason codes become plain labels here so Dalgo slices and filters read as words, and a
-- met chapter reads "Met" rather than blank (the metric model keeps NULL). The two "no volunteer"
-- columns stay here but are left out of the Dalgo table (always 0 today).
-- Shows the CHO only, as "Chapter organiser"; co_name stays in the metric model for the CO rollup.
--
-- Names are plain words for cold readers (spec §3.6c). Column order is the dashboard's story
-- (spec §3.5b), which Dalgo follows: children -> mentoring circles and slots -> volunteers ->
-- sessions. "Sessions held since 1 October" sums
-- prod_do_weekly_sessions (circle-sessions, §3.3b counting); it joins here rather than in the
-- summary because the weekly model already refs the summary.

with sessions_since_1_oct as (
    select
        chapter_id,
        sum(sessions_planned)::bigint as sessions_planned,
        sum(sessions_held)::bigint as sessions_held
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1
)

select
    s.chapter_name                                                      as "Chapter",
    s.city                                                              as "City",
    s.cho_name                                                          as "Chapter organiser",
    s.district                                                          as "District",
    s.do_name                                                           as "DO",
    -- Children
    s.active_children                                                   as "Active children",
    s.children_covered                                                  as "Children with a volunteer",
    s.pct_children_covered                                              as "% of children with a volunteer",
    s.children_in_circle_no_volunteer                                   as "Children in a mentoring circle with no volunteer",
    s.children_in_circle_no_slot                                        as "Children in a mentoring circle with no weekly slot",
    s.children_in_no_class_section                                      as "Children not yet in a mentoring circle",
    -- Mentoring circles and slots
    s.total_slots                                                       as "Weekly session slots",
    s.mentoring_circles_total                                           as "Mentoring circles",
    s.mentoring_circles_with_slot                                       as "Mentoring circles with a weekly slot",
    s.mentoring_circles_no_slot                                         as "Mentoring circles with no weekly slot",
    -- Volunteers
    s.volunteers_needed                                                 as "Volunteers needed (1:5)",
    s.volunteers_in_chapter                                             as "Volunteers signed up",
    s.volunteers_allocated                                              as "Volunteers placed in a mentoring circle",
    s.volunteers_not_allocated                                          as "Volunteers signed up but not placed",
    s.mentoring_circles_needing_volunteer                               as "Mentoring circles still needing a volunteer",
    s.mentoring_circles_0_volunteers                                    as "Mentoring circles with no volunteer",
    s.mentoring_circles_1_volunteer                                     as "Mentoring circles with 1 volunteer",
    s.mentoring_circles_2plus_volunteers                                as "Mentoring circles with 2 or more volunteers",
    case when s.volunteer_requirement_met_1to5 then 'Yes' else 'No' end as "Every mentoring circle has a volunteer (1:5)",
    case when s.volunteer_requirement_met_2to5 then 'Yes' else 'No' end as "Every mentoring circle has 2 volunteers (2:5)",
    case s.volunteer_requirement_not_met_reason
        when 'no_data' then 'No data this year'
        when 'no_slots' then 'No slots set up'
        when 'no_circles' then 'No mentoring circles set up'
        when 'circles_with_no_slot' then 'Some mentoring circles have no slot'
        when 'circles_without_volunteer' then 'Some circles have no volunteer'
        else 'Met'
    end                                                                 as "Why volunteer requirement not met",
    -- Sessions
    case when s.has_sessions_started then 'Yes' else 'No' end           as "Sessions have started",
    case when s.sessions_commenced_met then 'Yes' else 'No' end         as "Every mentoring circle has started",
    s.pct_mentoring_circles_started                                     as "% of mentoring circles started",
    s.mentoring_circles_not_yet_due                                     as "Mentoring circles with first session still ahead",
    s.mentoring_circles_not_started                                     as "Mentoring circles not started",
    s.mentoring_circles_no_session_since_1_oct                          as "Mentoring circles with sessions in September, none since 1 Oct",
    coalesce(w.sessions_planned, 0)                                     as "Sessions planned since 1 October",
    coalesce(w.sessions_held, 0)                                        as "Sessions held since 1 October",
    coalesce(w.sessions_planned, 0) - coalesce(w.sessions_held, 0)      as "Sessions with no attendance logged since 1 October",
    round(100.0 * w.sessions_held / nullif(w.sessions_planned, 0), 1)   as "% of planned sessions held since 1 October",
    case s.sessions_commenced_not_met_reason
        when 'no_data' then 'No data this year'
        when 'no_slots' then 'No slots set up'
        when 'no_circles' then 'No mentoring circles set up'
        when 'circles_not_started' then 'Some mentoring circles haven''t started'
        when 'circles_not_yet_due' then 'First session upcoming'
        else 'Met'
    end                                                                 as "Why not all mentoring circles started"
from {{ ref('prod_do_chapter_summary') }} s
left join sessions_since_1_oct w
    on s.chapter_id = w.chapter_id
