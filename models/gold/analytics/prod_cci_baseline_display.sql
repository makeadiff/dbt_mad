{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The CCI Baseline tab's table (spec E1 §7 B5), the same shape as School's
-- prod_do_baseline_display: one row per CCI from prod_cci_baseline_chapters, with the same filter
-- columns as prod_cci_chapters_display (CCI, City, District, DO). Grades 5 to 8 only (E1 spec §8
-- 2026-11-13): the base and children with a baseline are children in grades 5-8. Counts children,
-- not forms, except "Baseline forms submitted", which counts every form as the sheet does.

select
    chapter_name                    as "CCI",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    children_grades_5_to_8          as "Active children in grades 5 to 8",
    children_with_baseline          as "Children with a baseline",
    children_grades_5_to_8 - least(children_with_baseline, children_grades_5_to_8)
                                    as "Children with no baseline",
    pct_children_with_baseline      as "% of children with a baseline",
    children_cleared_aser           as "Children who cleared ASER",
    baseline_forms_submitted        as "Baseline forms submitted"
from {{ ref('prod_cci_baseline_chapters') }}
