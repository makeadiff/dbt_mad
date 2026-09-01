{{ config(materialized='table') }}

-- dim_date_week: weekly date spine for Row 4's intake + allocation trend (§8).
-- Grain: one row per ISO week start (Monday), 2023-01-02 through one year past today, covering
-- the full observed range of both the intake side (opportunity applications, back to 2023) and
-- the allocation side (slot_class_section_volunteer assignments).

with spine as (
    {{ dbt_utils.date_spine(
        datepart="week",
        start_date="cast('2023-01-02' as date)",
        end_date="cast(current_date + interval '1 year' as date)"
    ) }}
)

select
    date_week as week_start_date,
    (date_week + interval '6 days')::date as week_end_date,
    extract(isoyear from date_week)::integer as iso_year,
    extract(week from date_week)::integer as iso_week
from spine
