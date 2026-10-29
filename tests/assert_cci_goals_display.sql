-- Goals table checks (spec E1 §3.9, §7 D4): 20 rows (4 districts + All districts, x 4 goals); All
-- districts rows for goals 1-3 match prod_cci_district_display's All districts row; district counts
-- for goals 1-3 add up to the All districts counts; Met is Yes iff Now >= Target; goal 4 is blank
-- and waiting; "District order" is 0 for All districts then 8-11.
with g as (
    select * from {{ ref('prod_cci_goals_display') }}
),

all_row as (
    select * from {{ ref('prod_cci_district_display') }} where "District" = 'All districts'
),

sums as (
    select
        "Goal order" as goal_order,
        sum(split_part("Count", ' of ', 1)::numeric) filter (where "District" <> 'All districts') as dist_num,
        sum(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "District" <> 'All districts') as dist_den,
        max(replace(split_part("Count", ' of ', 1), ',', '')::numeric) filter (where "District" = 'All districts') as all_num,
        max(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "District" = 'All districts') as all_den
    from g
    where "Goal order" <= 3
    group by 1
)

select 'row count' as failed_check from (select count(*) n from g) c where n <> 20
union all
select 'goal 1 vs district display'
from g, all_row a
where g."District" = 'All districts' and g."Goal order" = 1
  and g."Now" is distinct from a."% of children with a volunteer"
union all
select 'goal 2 vs district display'
from g, all_row a
where g."District" = 'All districts' and g."Goal order" = 2
  and g."Now" is distinct from a."% of CCIs meeting the volunteer requirement"
union all
select 'goal 3 vs district display'
from g, all_row a
where g."District" = 'All districts' and g."Goal order" = 3
  and g."Now" is distinct from a."% of CCIs with every mentoring circle started"
union all
select 'goal ' || goal_order || ' counts add up'
from sums
where dist_num <> all_num or dist_den <> all_den
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
select 'district order'
from g
where "District order" is distinct from case "District"
    when 'All districts' then 0 when 'District 8' then 8 when 'District 9' then 9
    when 'District 10' then 10 when 'District 11' then 11 end
