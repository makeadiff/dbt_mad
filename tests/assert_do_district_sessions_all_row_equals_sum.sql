-- Sessions on the "All districts" row equal the sum of the district rows, and the sum of the
-- weekly rows (spec §3.6d).
with districts as (
    select sum(sessions_planned) as planned, sum(sessions_held) as held
    from {{ ref('prod_do_district_summary') }}
    where district <> 'All districts'
),

weekly as (
    select sum(sessions_planned) as planned, sum(sessions_held) as held
    from {{ ref('prod_do_weekly_sessions') }}
)

select a.district
from {{ ref('prod_do_district_summary') }} a
cross join districts d
cross join weekly w
where a.district = 'All districts'
  and (a.sessions_planned <> d.planned or a.sessions_held <> d.held
       or a.sessions_planned <> w.planned or a.sessions_held <> w.held)
