{{ config(materialized='table') }}

-- CCI dashboard (E1), Metric 4: baseline (spec E1 §3.7, rulings B1-B5 signed 2026-11-12). One row
-- per CCI, every CCI in prod_cci_chapter_summary (28), so a CCI with no forms on the sheet shows 0
-- (Bala Mandir, Arya Bal Grih, Sarada Niketan and any CCI not yet on the sheet).
--
-- Foundations only (Akshay, 2026-11-14): baseline is assessed in the Foundations programme only, so
-- the denominator is active children in Foundations (children_foundations, prod_cci_chapter_summary,
-- programme from the section's course), not all active children.
-- Children with a baseline on the sheet (B3): distinct (CCI, lower-trimmed name, grade) from the E1
-- baseline sheet, via int_google_sheet__cci_baseline_children. The sheet has no child ID, so a form
-- can't be tied to a programme; children with a baseline = the sheet count capped at the CCI's
-- Foundations children, so % never passes 100% and a CCI with no Foundations children counts 0
-- (St Mary's, HOH Girls, Mahila Mandiram, Vasavya had forms on 14 Nov). The uncapped count stays as
-- children_with_baseline_on_sheet so the gap stays visible.
-- Cleared ASER (B4): children with "Yes" to the ASER question on any form, capped at children with a
-- baseline.
-- Baseline forms submitted: every form row (a child assessed twice counts twice), as the sheet's
-- Dashboard tab counts "attempted".
--
-- Reaches into silver (int_google_sheet__*): no mart carries the baseline sheet, as on School
-- (prod_do_baseline_chapters reads int_dots__*).
-- Grain: one row per chapter_id.

with by_chapter as (
    select
        school_id                                   as chapter_id,
        count(*)                                    as children_on_sheet,
        count(*) filter (where cleared_aser)        as children_cleared_aser,
        sum(forms_submitted)                        as baseline_forms_submitted
    from {{ ref('int_google_sheet__cci_baseline_children') }}
    group by 1
),

capped as (
    select
        s.chapter_id,
        s.chapter_name,
        s.city,
        s.district,
        s.do_name,
        s.active_children,
        s.children_foundations,
        coalesce(b.children_on_sheet, 0)            as children_with_baseline_on_sheet,
        least(coalesce(b.children_on_sheet, 0), s.children_foundations)
                                                    as children_with_baseline,
        coalesce(b.children_cleared_aser, 0)        as children_cleared_aser_on_sheet,
        coalesce(b.baseline_forms_submitted, 0)     as baseline_forms_submitted
    from {{ ref('prod_cci_chapter_summary') }} s
    left join by_chapter b on s.chapter_id = b.chapter_id
)

select
    chapter_id,
    chapter_name,
    city,
    district,
    do_name,
    active_children,
    children_foundations,
    children_with_baseline_on_sheet,
    children_with_baseline,
    round(100.0 * children_with_baseline / nullif(children_foundations, 0), 1)
                                                    as pct_children_with_baseline,
    least(children_cleared_aser_on_sheet, children_with_baseline)
                                                    as children_cleared_aser,
    baseline_forms_submitted
from capped
