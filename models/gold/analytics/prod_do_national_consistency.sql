{{ config(materialized='table') }}

-- DO dashboard (E2): national Metric 3 per period, a ratio of sums across
-- prod_do_volunteer_consistency's 62 chapters. prod_do_national_kpis is one row, so per-period
-- national figures live here for a Dalgo tile that follows the Period filter.
--
-- Grain: one row per (period_type, period_start).

select
    period_type,
    period_label,
    period_sort,
    period_start,
    period_end,
    sum(volunteers_with_planned_sessions)::bigint as volunteers_with_planned_sessions,
    sum(volunteers_no_sessions_planned)::bigint as volunteers_no_sessions_planned,
    sum(volunteers_consistent)::bigint as volunteers_consistent,
    round(100.0 * sum(volunteers_consistent) / nullif(sum(volunteers_with_planned_sessions), 0), 1) as pct_volunteers_consistent,
    70 as target_pct_volunteers_consistent,
    sum(planned_sessions)::bigint as planned_sessions,
    sum(sessions_taken)::bigint as sessions_taken
from {{ ref('prod_do_volunteer_consistency') }}
group by 1, 2, 3, 4, 5
