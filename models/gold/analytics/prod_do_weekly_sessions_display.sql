{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter per week, sourced from prod_do_weekly_sessions.
-- Counts add up across weeks; "% of planned sessions held" is per row and never summed.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    ideal_sessions                  as "Ideal sessions",
    sessions_planned                as "Sessions planned",
    sessions_lost_to_late_starts    as "Sessions lost to late starts",
    sessions_held                   as "Sessions held",
    sessions_planned - sessions_held as "Sessions with no attendance logged",
    sessions_held_by_substitute     as "Sessions held by a substitute",
    pct_sessions_held               as "% of planned sessions held"
from {{ ref('prod_do_weekly_sessions') }}
