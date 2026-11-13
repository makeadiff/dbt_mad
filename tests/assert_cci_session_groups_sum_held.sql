-- Per chapter x week, the Held rows of the long model sum to the breakdown's sessions_held
-- (spec E1 §7 L5, pies).
with groups as (
    select "CCI" as chapter_name, "City" as city, "District" as district, "Programme" as programme, "Week starting" as week_start,
        sum("Sessions") as sessions
    from {{ ref('prod_cci_session_groups_display') }}
    where "What happened" = 'Held'
    group by 1, 2, 3, 4, 5
)

select b.chapter_id, b.week_start
from {{ ref('prod_cci_session_breakdown') }} b
left join groups g
    on b.chapter_name = g.chapter_name
   and b.city is not distinct from g.city
   and b.district is not distinct from g.district
   and b.programme = g.programme
   and b.week_start = g.week_start
where coalesce(g.sessions, 0) <> b.sessions_held
