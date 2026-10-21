{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter per period, sourced from
-- prod_do_volunteer_consistency; DOs pick one period from the filter (consistency is per
-- volunteer over the whole period, so periods don't add up).

select
    chapter_name                                                    as "Chapter",
    city                                                            as "City",
    cho_name                                                        as "Chapter organiser",
    district                                                        as "District",
    do_name                                                         as "DO",
    period_label                                                    as "Period",
    period_sort                                                     as "Period order",
    volunteers_with_planned_sessions                                as "Volunteers with sessions planned",
    volunteers_consistent                                           as "Consistent volunteers",
    pct_volunteers_consistent                                       as "% of volunteers consistent",
    volunteers_no_sessions_planned                                  as "Volunteers with no sessions planned yet",
    case when below_target then 'Yes' else 'No' end                 as "Below 70% target"
from {{ ref('prod_do_volunteer_consistency') }}
