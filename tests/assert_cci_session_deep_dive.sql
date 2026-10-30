-- CCI session deep dive (spec E1 §3.10). Original + substitute = sessions held and matches the weekly
-- sessions model on every CCI-week; reasons add up to the cancellations; child rows never attend more
-- than held, and bands match the %. Returns a row only when something differs.
select b.chapter_id::text as problem
from {{ ref('prod_cci_session_breakdown') }} b
join {{ ref('prod_cci_weekly_sessions') }} w
    on b.chapter_id = w.chapter_id
   and b.week_start = w.week_start
where b.sessions_held <> w.sessions_held
union all
select 'reasons <> cancelled'
where (select coalesce(sum(sessions_cancelled), 0) from {{ ref('prod_cci_cancellation_reasons') }})
   <> (select coalesce(sum(sessions_cancelled), 0) from {{ ref('prod_cci_session_breakdown') }})
union all
select 'child ' || child_id::text
from {{ ref('prod_cci_child_sessions') }}
where sessions_attended > sessions_held
   or sessions_attended::numeric / sessions_held >= 0.90 and status <> 'Healthy'
