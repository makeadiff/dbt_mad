{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The CCI Baseline tab's table (spec E1 §7 B5), the same shape as School's
-- prod_do_baseline_display: one row per CCI from prod_cci_baseline_chapters, with the same filter
-- columns as prod_cci_chapters_display (CCI, City, District, DO). Foundations only (2026-11-14):
-- baseline is assessed in Foundations, so the base is active children in Foundations and children
-- with a baseline are capped at it; "Children with a baseline on the sheet" is the uncapped count.
-- Counts children, not forms, except "Baseline forms submitted", which counts forms as the sheet does.

select
    chapter_name                    as "CCI",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    children_foundations            as "Foundations children",
    children_with_baseline          as "Children with a baseline",
    children_foundations - children_with_baseline
                                    as "Children with no baseline",
    pct_children_with_baseline      as "% of children with a baseline",
    children_cleared_aser           as "Children who cleared ASER",
    baseline_forms_submitted        as "Baseline forms submitted",
    children_with_baseline_on_sheet as "Children with a baseline on the sheet"
from {{ ref('prod_cci_baseline_chapters') }}
