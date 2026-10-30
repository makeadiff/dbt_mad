-- Catch-up columns (spec E1 §3.11): on every CCI, circles with 0 + 1 + 2+ volunteers = mentoring circles;
-- and the district rows and the "All districts" row add up to the chapters on each new column.
-- Returns a row only when something differs.
with chapter_bad as (
    select chapter_id::text as problem
    from {{ ref('prod_cci_chapter_summary') }}
    where circles_no_volunteer + circles_one_volunteer + circles_two_plus_volunteers <> mentoring_circles
       or circles_started + circles_not_started <> mentoring_circles
       or circles_stopped_logging > circles_started
),

chapters as (
    select
        sum(circles_one_volunteer) as one_v,
        sum(circles_two_plus_volunteers) as two_v,
        sum(circles_needing_volunteer) as needing,
        sum(circles_not_started) as not_started,
        sum(circles_stopped_logging) as stopped
    from {{ ref('prod_cci_chapter_summary') }}
),

districts as (
    select
        sum(circles_one_volunteer) as one_v,
        sum(circles_two_plus_volunteers) as two_v,
        sum(circles_needing_volunteer) as needing,
        sum(circles_not_started) as not_started,
        sum(circles_stopped_logging) as stopped
    from {{ ref('prod_cci_district_summary') }}
    where district <> 'All districts'
),

all_row as (
    select * from {{ ref('prod_cci_district_summary') }} where district = 'All districts'
),

rollup_bad as (
    select 'rollup' as problem
    from chapters c
    cross join districts d
    cross join all_row a
    where (c.one_v, c.two_v, c.needing, c.not_started, c.stopped) is distinct from (d.one_v, d.two_v, d.needing, d.not_started, d.stopped)
       or (c.one_v, c.two_v, c.needing, c.not_started, c.stopped) is distinct from (a.circles_one_volunteer, a.circles_two_plus_volunteers, a.circles_needing_volunteer, a.circles_not_started, a.circles_stopped_logging)
),

district_bad as (
    select district::text as problem
    from {{ ref('prod_cci_district_summary') }}
    where circles_no_volunteer + circles_one_volunteer + circles_two_plus_volunteers <> mentoring_circles
)

select * from chapter_bad
union all select * from rollup_bad
union all select * from district_bad
