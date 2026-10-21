{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per week for the national line chart: a ratio of sums across
-- prod_do_weekly_sessions' 62 chapters.

select
    week_label                                                              as "Week",
    week_start                                                              as "Week starting",
    sum(ideal_sessions)::bigint                                             as "Ideal sessions",
    sum(sessions_planned)::bigint                                           as "Sessions planned",
    sum(sessions_lost_to_late_starts)::bigint                               as "Sessions lost to late starts",
    sum(sessions_held)::bigint                                              as "Sessions held",
    sum(sessions_held_by_substitute)::bigint                                as "Sessions held by a substitute",
    round(100.0 * sum(sessions_held) / nullif(sum(sessions_planned), 0), 1) as "% of planned sessions held"
from {{ ref('prod_do_weekly_sessions') }}
group by 1, 2
