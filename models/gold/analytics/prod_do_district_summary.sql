{{ config(materialized='table') }}

-- DO dashboard (E2): one row per district plus an "All districts" row (spec §3.6b row 2), because
-- Dalgo can't total a table. Rolled up from prod_do_chapter_summary; every % is a ratio of sums.
-- One DO per district (seed_do_chapter_district), so do_name rides along; the All districts row
-- has none. district_sort puts the districts in number order and All districts last (8), since
-- "All districts" would sort first by name. Baseline columns come with Metric 4.
--
-- Grain: one row per district, plus one 'All districts' row.

with chapters as (
    select * from {{ ref('prod_do_chapter_summary') }}
),

rolled as (
    select
        district,
        do_name,
        substring(district from '[0-9]+')::int as district_sort,
        count(*) as chapters,
        sum(active_children)::bigint as active_children,
        sum(children_covered)::bigint as children_covered,
        count(*) filter (where total_slots = 0) as chapters_no_slots,
        sum(volunteers_in_chapter)::bigint as volunteers_in_chapter,
        sum(volunteers_allocated)::bigint as volunteers_allocated,
        count(*) filter (where volunteer_requirement_met_1to5) as chapters_met_1to5,
        sum(mentoring_circles_started)::bigint as mentoring_circles_started,
        sum(mentoring_circles_started + mentoring_circles_not_started)::bigint as mentoring_circles_due
    from chapters
    group by 1, 2

    union all

    select
        'All districts',
        null,
        8,
        count(*),
        sum(active_children)::bigint,
        sum(children_covered)::bigint,
        count(*) filter (where total_slots = 0),
        sum(volunteers_in_chapter)::bigint,
        sum(volunteers_allocated)::bigint,
        count(*) filter (where volunteer_requirement_met_1to5),
        sum(mentoring_circles_started)::bigint,
        sum(mentoring_circles_started + mentoring_circles_not_started)::bigint
    from chapters
)

select
    district,
    do_name,
    district_sort,
    chapters,
    active_children,
    children_covered,
    round(100.0 * children_covered / nullif(active_children, 0), 1) as pct_children_covered,
    chapters_no_slots,
    volunteers_in_chapter,
    volunteers_allocated,
    round(100.0 * volunteers_allocated / nullif(volunteers_in_chapter, 0), 1) as pct_volunteers_placed,
    chapters_met_1to5,
    round(100.0 * chapters_met_1to5 / nullif(chapters, 0), 1) as pct_chapters_met_1to5,
    mentoring_circles_started,
    mentoring_circles_due,
    round(100.0 * mentoring_circles_started / nullif(mentoring_circles_due, 0), 1) as pct_mentoring_circles_started
from rolled
