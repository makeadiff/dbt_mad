{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The 31 Oct goals table (spec §3.6e): one row per district x goal, plus
-- "All districts" x goal (5 goals x 8 = 40 rows), so the District / DO filters narrow it and an
-- unfiltered table opens on the national rows ("District order": All districts 0, then 1-7).
-- Counts come from the existing models: prod_do_district_summary (goals 1 and 2),
-- prod_do_chapter_summary (goal 3, chapters with every circle started), and
-- prod_do_volunteer_consistency, period "Since 1 October", rolled up by district (goal 4; no
-- district rollup existed). Goal 5 (baseline) has no source yet: Not measured yet.
-- Met = Now >= Target.

with districts as (
    select district, do_name, district_sort, chapters, active_children, children_covered, chapters_met_1to5
    from {{ ref('prod_do_district_summary') }}
),

started as (
    select district, count(*) filter (where sessions_commenced_met) as chapters_all_started
    from {{ ref('prod_do_chapter_summary') }}
    group by 1
    union all
    select 'All districts', count(*) filter (where sessions_commenced_met)
    from {{ ref('prod_do_chapter_summary') }}
),

consistency as (
    select
        district,
        sum(volunteers_consistent)::bigint as consistent,
        sum(volunteers_with_planned_sessions)::bigint as with_planned
    from {{ ref('prod_do_volunteer_consistency') }}
    where period_type = 'since_1_oct'
    group by 1
    union all
    select 'All districts', sum(volunteers_consistent)::bigint, sum(volunteers_with_planned_sessions)::bigint
    from {{ ref('prod_do_volunteer_consistency') }}
    where period_type = 'since_1_oct'
),

goals as (
    select d.district, d.do_name, d.district_sort, 1 as goal_order,
        '% of children who have a volunteer' as goal, 100 as target,
        d.children_covered::bigint as num, d.active_children::bigint as den
    from districts d
    union all
    select d.district, d.do_name, d.district_sort, 2,
        '% of chapters where every mentoring circle has a volunteer', 80,
        d.chapters_met_1to5::bigint, d.chapters::bigint
    from districts d
    union all
    select d.district, d.do_name, d.district_sort, 3,
        '% of chapters where every mentoring circle has started', 80,
        coalesce(s.chapters_all_started, 0)::bigint, d.chapters::bigint
    from districts d
    left join started s on s.district = d.district
    union all
    select d.district, d.do_name, d.district_sort, 4,
        '% of volunteers who held 9 in 10 of their planned sessions since 1 October', 70,
        coalesce(c.consistent, 0)::bigint, coalesce(c.with_planned, 0)::bigint
    from districts d
    left join consistency c on c.district = d.district
    union all
    select d.district, d.do_name, d.district_sort, 5,
        '% of children with a baseline done', 80, null, null
    from districts d
),

scored as (
    select *, round(100.0 * num / nullif(den, 0), 1) as now_pct
    from goals
)

select
    district                                        as "District",
    do_name                                         as "DO",
    goal                                            as "Goal",
    goal_order                                      as "Goal order",
    target::text || '%'                             as "Target",
    now_pct                                         as "Now",
    case when den is null then null
         else to_char(num, 'FM999,999,999') || ' of ' || to_char(den, 'FM999,999,999')
    end                                             as "Count",
    case when goal_order = 5 then 'Not measured yet'
         when now_pct >= target then 'Yes'
         else 'No'
    end                                             as "Met",
    '31 Oct 2026'                                   as "Due",
    case when district = 'All districts' then 0 else district_sort end as "District order"
from scored
