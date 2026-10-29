{{ config(severity='warn') }}

-- Expected national values from the spec E1 §4 pre-check (29 Oct 2026). Warn, never error: live data
-- moves and a changed count must not fail a prod run. A warning means re-check, not that the model
-- is wrong. Volunteer figures also drift because prod staging is a nightly snapshot of a streaming
-- source.
with actual as (
    select
        count(*) as ccis,
        count(*) filter (where has_children) as ccis_with_children,
        sum(active_children) as active_children,
        sum(mentoring_circles) as mentoring_circles,
        sum(circles_no_volunteer) as circles_no_volunteer,
        sum(sections_no_circle) as sections_no_circle,
        sum(children_with_volunteer) as children_with_volunteer,
        sum(volunteers_placed) as volunteers_placed,
        sum(circles_started) as circles_started,
        sum(sessions_held_since_1_oct) as sessions_held_since_1_oct
    from {{ ref('prod_cci_chapter_summary') }}
)

select *
from actual
where ccis <> 28
   or ccis_with_children <> 27
   or active_children <> 1123
   or mentoring_circles <> 228
   or circles_no_volunteer <> 20
   or sections_no_circle <> 175
   or children_with_volunteer <> 387
   or volunteers_placed <> 220
   or circles_started <> 75
   or sessions_held_since_1_oct <> 107
