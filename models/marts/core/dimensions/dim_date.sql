{{ config(materialized='table') }}

-- dim_date: day-grain calendar spine, 2023-01-01 through one year past today.
-- Grain: one row per calendar date.
--
-- This exists because the recruitment campaign board reports day by day: a chapter that
-- recruited nobody on a Wednesday must render a 0, not a missing column. Only a spine can
-- produce a row for a day that has no underlying records, and a pivot table with ragged columns
-- is unreadable as a leaderboard.
--
-- (2026-09-13: a week-grain sibling, dim_date_week, was built for SRI_DASHBOARD_SPEC.md §8
-- Row 4's weekly trend but never got a consumer -- removed. If that weekly trend gets built,
-- rebuild it as its own model rather than widening this one's grain to weekly.)

with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast(current_date + interval '1 year' as date)"
    ) }}
)

select
    date_day::date as date_key,
    extract(isodow from date_day)::integer as iso_day_of_week,
    to_char(date_day, 'Dy') as day_short_name,
    to_char(date_day, 'DD Mon') as day_label,
    (date_trunc('week', date_day))::date as week_start_date,
    extract(isoyear from date_day)::integer as iso_year,
    extract(week from date_day)::integer as iso_week,
    extract(month from date_day)::integer as month_number,
    extract(isodow from date_day) in (6, 7) as is_weekend
from spine
