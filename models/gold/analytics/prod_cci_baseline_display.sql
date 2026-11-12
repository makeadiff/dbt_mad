{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The CCI Baseline tab's table (spec E1 §7 B5), the same shape as School's
-- prod_do_baseline_display: one row per CCI from prod_cci_baseline_chapters, with the same filter
-- columns as prod_cci_chapters_display (CCI, City, District, DO). Counts children, not forms (one-line
-- note on the tab), except "Baseline forms submitted", which counts forms as the sheet does.

select
    chapter_name                    as "CCI",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    active_children                 as "Active children",
    children_with_baseline          as "Children with a baseline",
    active_children - least(children_with_baseline, active_children)
                                    as "Children with no baseline",
    pct_children_with_baseline      as "% of children with a baseline",
    children_cleared_aser           as "Children who cleared ASER",
    baseline_forms_submitted        as "Baseline forms submitted"
from {{ ref('prod_cci_baseline_chapters') }}
