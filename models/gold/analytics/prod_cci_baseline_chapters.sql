{{ config(materialized='table') }}

-- CCI dashboard (E1), Metric 4: baseline (spec E1 §3.7, rulings B1-B5 signed 2026-11-12). One row
-- per CCI, every CCI in prod_cci_chapter_summary (28), so a CCI with no forms on the sheet shows 0
-- (Bala Mandir, Arya Bal Grih, Sarada Niketan and any CCI not yet on the sheet).
--
-- Children with a baseline (B3): distinct (CCI, lower-trimmed name, grade) from the E1 baseline
-- sheet, via int_google_sheet__cci_baseline_children. % with a baseline = that / active children
-- (§3.0, prod_cci_chapter_summary), not the sheet's FP count, so it differs from the sheet's %, and
-- can pass 100% where the sheet lists children Platform Commons doesn't have as active.
-- Cleared ASER (B4): children with "Yes" to the ASER question on any form.
-- Baseline forms submitted: every form row (a child assessed twice counts twice), as the sheet's
-- Dashboard tab counts "attempted".
--
-- Reaches into silver (int_google_sheet__*): no mart carries the baseline sheet, as on School
-- (prod_do_baseline_chapters reads int_dots__*).
-- Grain: one row per chapter_id.

with by_chapter as (
    select
        school_id                                   as chapter_id,
        count(*)                                    as children_with_baseline,
        count(*) filter (where cleared_aser)        as children_cleared_aser,
        sum(forms_submitted)                        as baseline_forms_submitted
    from {{ ref('int_google_sheet__cci_baseline_children') }}
    group by 1
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    s.active_children,
    coalesce(b.children_with_baseline, 0)           as children_with_baseline,
    round(100.0 * coalesce(b.children_with_baseline, 0) / nullif(s.active_children, 0), 1)
                                                    as pct_children_with_baseline,
    coalesce(b.children_cleared_aser, 0)            as children_cleared_aser,
    coalesce(b.baseline_forms_submitted, 0)         as baseline_forms_submitted
from {{ ref('prod_cci_chapter_summary') }} s
left join by_chapter b on s.chapter_id = b.chapter_id
