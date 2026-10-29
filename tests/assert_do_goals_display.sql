-- Goals table checks (spec §3.6e): 40 rows (8 districts x 5 goals); All districts goals 1-3 match
-- prod_do_district_display's All row (goal 3 from prod_do_chapter_summary); goal 4 All row matches prod_do_national_consistency_display
-- "Since 1 October"; district goal-4 counts add up to the All row; Met is Yes iff Now >= Target.
with g as (
    select * from {{ ref('prod_do_goals_display') }}
),

all_district as (
    select * from {{ ref('prod_do_district_display') }} where "District" = 'All districts'
),

nat as (
    select * from {{ ref('prod_do_national_consistency_display') }} where "Period" = 'Since 1 October'
),

g4 as (
    select
        sum(split_part("Count", ' of ', 1)::numeric) filter (where "District" <> 'All districts') as dist_num,
        sum(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "District" <> 'All districts') as dist_den,
        max(replace(split_part("Count", ' of ', 1), ',', '')::numeric) filter (where "District" = 'All districts') as all_num,
        max(replace(split_part("Count", ' of ', 2), ',', '')::numeric) filter (where "District" = 'All districts') as all_den
    from g where "Goal order" = 4
)

select 'row count' as failed_check from (select count(*) n from g) c where n <> 40
union all
select 'goal 1'
from g, all_district a
where g."District" = 'All districts' and g."Goal order" = 1
  and g."Now" is distinct from a."% of children with a volunteer"
union all
select 'goal 2'
from g, all_district a
where g."District" = 'All districts' and g."Goal order" = 2
  and g."Now" is distinct from a."% of chapters where every mentoring circle has a volunteer"
union all
select 'goal 3'
from g
where g."District" = 'All districts' and g."Goal order" = 3
  and g."Now" is distinct from (
      select round(100.0 * count(*) filter (where sessions_commenced_met) / count(*), 1)
      from {{ ref('prod_do_chapter_summary') }})
union all
select 'goal 4 vs national'
from g, nat n
where g."District" = 'All districts' and g."Goal order" = 4
  and (g."Now" is distinct from n."% of volunteers consistent"
       or g."Count" <> to_char(n."Consistent volunteers", 'FM999,999,999') || ' of '
                       || to_char(n."Volunteers with sessions planned", 'FM999,999,999'))
union all
select 'goal 4 districts sum'
from g4
where dist_num <> all_num or dist_den <> all_den
union all
select 'met flag'
from g
where "Goal order" <> 5 and ("Met" = 'Yes') <> ("Now" >= replace("Target", '%', '')::numeric)
