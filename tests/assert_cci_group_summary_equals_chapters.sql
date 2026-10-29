-- The groups add up to the chapters, and the "All" row equals the sum of the chapters on every
-- count (spec E1 §2, §3.8). Returns a row only when something differs.
with chapters as (
    select
        sum(active_children) as active_children,
        sum(children_with_volunteer) as children_with_volunteer,
        sum(mentoring_circles) as mentoring_circles,
        sum(volunteers_placed) as volunteers_placed,
        sum(circles_started) as circles_started,
        sum(sessions_held_since_1_oct) as sessions_held_since_1_oct,
        count(*) filter (where volunteer_requirement_met_1to5) as ccis_requirement_met_1to5
    from {{ ref('prod_cci_chapter_summary') }}
),

groups as (
    select
        sum(active_children) as active_children,
        sum(children_with_volunteer) as children_with_volunteer,
        sum(mentoring_circles) as mentoring_circles,
        sum(volunteers_placed) as volunteers_placed,
        sum(circles_started) as circles_started,
        sum(sessions_held_since_1_oct) as sessions_held_since_1_oct,
        sum(ccis_requirement_met_1to5) as ccis_requirement_met_1to5
    from {{ ref('prod_cci_group_summary') }}
    where group_name <> 'All'
),

all_row as (
    select * from {{ ref('prod_cci_group_summary') }}
    where group_name = 'All'
)

select a.group_name
from all_row a
cross join groups g
cross join chapters c
where a.active_children <> c.active_children or g.active_children <> c.active_children
   or a.children_with_volunteer <> c.children_with_volunteer or g.children_with_volunteer <> c.children_with_volunteer
   or a.mentoring_circles <> c.mentoring_circles or g.mentoring_circles <> c.mentoring_circles
   or a.volunteers_placed <> c.volunteers_placed or g.volunteers_placed <> c.volunteers_placed
   or a.circles_started <> c.circles_started or g.circles_started <> c.circles_started
   or a.sessions_held_since_1_oct <> c.sessions_held_since_1_oct or g.sessions_held_since_1_oct <> c.sessions_held_since_1_oct
   or a.ccis_requirement_met_1to5 <> c.ccis_requirement_met_1to5 or g.ccis_requirement_met_1to5 <> c.ccis_requirement_met_1to5
