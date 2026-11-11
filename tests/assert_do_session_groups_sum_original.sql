-- Per chapter x week, the "Original volunteer" rows of the long model sum to the breakdown's
-- sessions_held_by_original, and "Held by" is set exactly on Held rows (spec §3.8, pies).
with groups as (
    select "Chapter" as chapter_name, "City" as city, "District" as district, "Week starting" as week_start,
        sum("Sessions") as sessions
    from {{ ref('prod_do_session_groups_display') }}
    where "Held by" = 'Original volunteer'
    group by 1, 2, 3, 4
)

select b.chapter_id::text as problem
from {{ ref('prod_do_session_breakdown') }} b
left join groups g
    on b.chapter_name = g.chapter_name
   and b.city is not distinct from g.city
   and b.district is not distinct from g.district
   and b.week_start = g.week_start
where coalesce(g.sessions, 0) <> b.sessions_held_by_original
union all
select 'Held by set on a non-Held row, or missing on a Held row'
from {{ ref('prod_do_session_groups_display') }}
where ("What happened" = 'Held') <> ("Held by" is not null)
limit 1
