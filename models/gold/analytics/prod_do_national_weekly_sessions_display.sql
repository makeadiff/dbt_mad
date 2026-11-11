{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per week for the national line chart: a ratio of sums across
-- prod_do_weekly_sessions' 62 chapters. "Month" as on the chapter weekly model.

select
    week_label                                                              as "Week",
    week_start                                                              as "Week starting",
    month_label                                                             as "Month",
    month_start                                                             as "Month starting",
    sum(sessions_planned)::bigint                                           as "Sessions planned",
    sum(sessions_held)::bigint                                              as "Sessions held",
    sum(sessions_held_by_substitute)::bigint                                as "Sessions held by a substitute",
    sum(sessions_cancelled)::bigint                                         as "Cancelled",
    round(100.0 * sum(sessions_held) / nullif(sum(sessions_planned), 0), 1) as "% of planned sessions held",
    round(100.0 * sum(sessions_cancelled) / nullif(sum(sessions_planned), 0), 1) as "% of planned sessions cancelled"
from {{ ref('prod_do_weekly_sessions') }}
group by 1, 2, 3, 4
