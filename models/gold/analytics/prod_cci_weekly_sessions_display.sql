{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI per week, sourced from prod_cci_weekly_sessions. Counts
-- add up across weeks. Planned, ideal and % held are added once the weekly-slot link is fixed.

select
    chapter_name    as "Chapter",
    city            as "City",
    district        as "District",
    do_name         as "DO",
    week_label      as "Week",
    week_start      as "Week starting",
    sessions_held   as "Sessions held"
from {{ ref('prod_cci_weekly_sessions') }}
