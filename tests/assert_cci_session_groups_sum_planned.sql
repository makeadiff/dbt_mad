-- Per chapter x week, "Sessions" summed over the long model equals the breakdown's planned
-- (spec E1 §7 L5, pies). Chapters match on name, city and district since the display has no id.
with groups as (
    select "CCI" as chapter_name, "City" as city, "District" as district, "Week starting" as week_start,
        sum("Sessions") as sessions
    from {{ ref('prod_cci_session_groups_display') }}
    group by 1, 2, 3, 4
)

select b.chapter_id, b.week_start
from {{ ref('prod_cci_session_breakdown') }} b
left join groups g
    on b.chapter_name = g.chapter_name
   and b.city is not distinct from g.city
   and b.district is not distinct from g.district
   and b.week_start = g.week_start
where coalesce(g.sessions, 0) <> b.sessions_planned
