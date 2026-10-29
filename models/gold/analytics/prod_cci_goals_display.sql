{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The 31 Oct goals table (spec E1 §3.9): one row per group x goal, plus
-- "All" x goal (14 x 4 = 56 rows), so the Group filter narrows it and an unfiltered table opens on
-- the All rows ("Group order": All 0, then groups A-Z). Groups with no CCIs with children (Guntur)
-- are left out. Everything comes from prod_cci_group_summary, whose % columns are ratios of sums;
-- nothing is recalculated here. Goal 4 (consistency) waits on the weekly-slot link (spec §5, §7
-- ruling 1): Now and Count blank. There is no baseline goal (spec §3.7).
-- Met = Now >= Target.

with groups as (
    select
        group_name,
        ccis,
        active_children,
        children_with_volunteer,
        pct_children_with_volunteer,
        ccis_requirement_met_1to5,
        pct_ccis_requirement_met_1to5,
        ccis_all_circles_started,
        pct_ccis_all_circles_started,
        case when group_name = 'All' then 0
             else row_number() over (partition by group_name = 'All' order by group_name)
        end as group_sort
    from {{ ref('prod_cci_group_summary') }}
    where ccis > 0
),

goals as (
    select group_name, group_sort, 1 as goal_order,
        '% of children who have a volunteer' as goal, 100 as target,
        children_with_volunteer::bigint as num, active_children::bigint as den,
        pct_children_with_volunteer as now_pct
    from groups
    union all
    select group_name, group_sort, 2,
        '% of CCIs where every mentoring circle has a volunteer', 80,
        ccis_requirement_met_1to5::bigint, ccis::bigint, pct_ccis_requirement_met_1to5
    from groups
    union all
    select group_name, group_sort, 3,
        '% of CCIs where every mentoring circle has started', 80,
        ccis_all_circles_started::bigint, ccis::bigint, pct_ccis_all_circles_started
    from groups
    union all
    select group_name, group_sort, 4,
        '% of volunteers who held 9 in 10 of their planned sessions since 1 October', 70,
        null, null, null
    from groups
)

select
    group_name                                      as "Group",
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
    group_sort                                      as "Group order"
from goals
