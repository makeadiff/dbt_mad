-- Reasons add up to the cancelled sessions; every volunteer and child row has taken/attended no more
-- than planned/held, and its status matches its %. Returns a row only when something differs
-- (spec DO §3.7 R3, R4).
with reasons as (
    select (select coalesce(sum(sessions_cancelled), 0) from {{ ref('prod_do_cancellation_reasons') }}) as r,
           (select coalesce(sum(sessions_cancelled), 0) from {{ ref('prod_do_session_breakdown') }}) as b
)
select 'reasons <> cancelled' as problem from reasons where r <> b
union all
select 'volunteer ' || volunteer_id::text from {{ ref('prod_do_volunteer_sessions') }}
where sessions_taken > sessions_planned
   or status <> case when attendance_pct >= 90 then 'Healthy' when attendance_pct >= 75 then 'At risk' else 'Unhealthy' end
   or sessions_taken::numeric / sessions_planned >= 0.90 and status <> 'Healthy'
union all
select 'child ' || child_id::text from {{ ref('prod_do_child_sessions') }}
where sessions_attended > sessions_held
   or sessions_attended::numeric / sessions_held >= 0.90 and status <> 'Healthy'
