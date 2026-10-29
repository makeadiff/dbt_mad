-- Goals table checks (spec E1 §3.9): 56 rows (13 groups + All, x 4 goals); All rows for goals 1-3
-- match prod_cci_group_display's All row; group counts for goals 1-3 add up to the All counts;
-- Met is Yes iff Now >= Target; goal 4 is blank and waiting; Guntur (no CCIs with children) is out.
with g as (
    select * from {{ ref('prod_cci_goals_display') }}
),

all_group as (
    select * from {{ ref('prod_cci_group_display') }} where "Group" = 'All'
),

sums as (
    select
        "Goal order" as goal_order,
        sum(split_part("Count", ' of ', 1)::numeric) filter (where "Group" <> 'All') as grp_num,
        sum(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "Group" <> 'All') as grp_den,
        max(replace(split_part("Count", ' of ', 1), ',', '')::numeric) filter (where "Group" = 'All') as all_num,
        max(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "Group" = 'All') as all_den
    from g
    where "Goal order" <= 3
    group by 1
)

select 'row count' as failed_check from (select count(*) n from g) c where n <> 56
union all
select 'goal 1 vs group display'
from g, all_group a
where g."Group" = 'All' and g."Goal order" = 1
  and g."Now" is distinct from a."% of children with a volunteer"
union all
select 'goal 2 vs group display'
from g, all_group a
where g."Group" = 'All' and g."Goal order" = 2
  and g."Now" is distinct from a."% of CCIs meeting the volunteer requirement"
union all
select 'goal 3 vs group display'
from g, all_group a
where g."Group" = 'All' and g."Goal order" = 3
  and g."Now" is distinct from a."% of CCIs with every mentoring circle started"
union all
select 'goal ' || goal_order || ' counts add up'
from sums
where grp_num <> all_num or grp_den <> all_den
union all
select 'met flag'
from g
where "Goal order" <= 3 and ("Met" = 'Yes') <> ("Now" >= replace("Target", '%', '')::numeric)
union all
select 'goal 4'
from g
where "Goal order" = 4
  and ("Now" is not null or "Count" is not null or "Met" <> 'Waiting on Platform Commons')
union all
select 'group with no children'
from g
where "Group" = 'Guntur'
union all
select 'group order'
from g
where ("Group" = 'All') <> ("Group order" = 0)
