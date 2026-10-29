{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The 31 Oct goals table (spec E1 §3.9, §7 D3-D4): one row per district x
-- goal, plus "All districts" x goal (5 x 4 = 20 rows), so the District / DO filters narrow it and an
-- unfiltered table opens on the All districts rows ("District order": All districts 0, then
-- 8-11). Everything comes from prod_cci_district_summary, whose % columns are ratios of sums;
-- nothing is recalculated here. Goal 4 (consistency) waits on the weekly-slot link (spec §5, §7
-- ruling 1): Now and Count blank. There is no baseline goal (spec §3.7).
-- Met = Now >= Target.

with districts as (
    select
        district,
        do_name,
        case when district = 'All districts' then 0 else district_sort end as district_sort,
        ccis,
        active_children,
        children_with_volunteer,
        pct_children_with_volunteer,
        ccis_requirement_met_1to5,
        pct_ccis_requirement_met_1to5,
        ccis_all_circles_started,
        pct_ccis_all_circles_started
    from {{ ref('prod_cci_district_summary') }}
),

goals as (
    select district, do_name, district_sort, 1 as goal_order,
        '% of children who have a volunteer' as goal, 100 as target,
        children_with_volunteer::bigint as num, active_children::bigint as den,
        pct_children_with_volunteer as now_pct
    from districts
    union all
    select district, do_name, district_sort, 2,
        '% of CCIs where every mentoring circle has a volunteer', 80,
        ccis_requirement_met_1to5::bigint, ccis::bigint, pct_ccis_requirement_met_1to5
    from districts
    union all
    select district, do_name, district_sort, 3,
        '% of CCIs where every mentoring circle has started', 80,
        ccis_all_circles_started::bigint, ccis::bigint, pct_ccis_all_circles_started
    from districts
    union all
    select district, do_name, district_sort, 4,
        '% of volunteers who held 9 in 10 of their planned sessions since 1 October', 70,
        null, null, null
    from districts
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
    case when goal_order = 4 then 'Waiting on Platform Commons'
         when now_pct >= target then 'Yes'
         else 'No'
    end                                             as "Met",
    '31 Oct 2026'                                   as "Due",
    district_sort                                   as "District order"
from goals
