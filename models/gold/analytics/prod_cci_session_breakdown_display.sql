{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI per week, sourced from prod_cci_session_breakdown (spec
-- E1 §3.10, layout v4 §7 L1 and L6). Counts add up across weeks and within a month; the % columns
-- are % of planned, per row, never summed. The first pie's groups are "Sessions held", "Cancelled",
-- "Volunteer absent" and "No volunteer assigned" (they add up to "Sessions planned"); the second
-- pie's are "Held by the original volunteer" and "Held by a substitute" (they add up to "Sessions held").

select
    chapter_name                    as "Chapter",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    week_label                      as "Week",
    week_start                      as "Week starting",
    month_label                     as "Month",
    month_start                     as "Month starting",
    sessions_planned                as "Sessions planned",
    sessions_held                   as "Sessions held",
    sessions_cancelled              as "Cancelled",
    sessions_volunteer_absent       as "Volunteer absent",
    sessions_no_volunteer_assigned  as "No volunteer assigned",
    sessions_held_by_original       as "Held by the original volunteer",
    sessions_held_by_substitute     as "Held by a substitute",
    pct_held_by_original            as "% of planned sessions held by the original volunteer",
    pct_held_by_substitute          as "% of planned sessions held by a substitute",
    pct_held                        as "% of planned sessions held",
    pct_cancelled                   as "% of planned sessions cancelled"
from {{ ref('prod_cci_session_breakdown') }}
