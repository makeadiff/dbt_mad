{{ config(materialized='table') }}

-- CCI dashboard (E1), Metric 4: baseline (spec E1 §3.7, rulings B1-B5 signed 2026-11-12). One row
-- per CCI, every CCI in prod_cci_chapter_summary (28), so a CCI with no forms on the sheet shows 0
-- (Bala Mandir, Arya Bal Grih, Sarada Niketan and any CCI not yet on the sheet).
--
-- Grades 5 to 8 only (E1 spec §8 2026-11-13, ruled by Akshay; replaces B3's denominator): baseline
-- is assessed in Foundations (grades 5-7) and grade 8.
-- Base: active children in grades 5-8, from the section's level name on Platform Commons
-- (prod_cci_active_children.grade); a child counts once per CCI. Children with no grade on Platform
-- Commons drop out. Every other CCI metric keeps all active children (active_children, §3.0).
-- Children with a baseline (B3): distinct (CCI, lower-trimmed name, grade) from the E1 baseline
-- sheet, via int_google_sheet__cci_baseline_children, only rows whose Student Grade is 5-8.
-- % with a baseline = that / the grades 5-8 base.
-- Cleared ASER (B4): children in grades 5-8 with "Yes" to the ASER question on any form.
-- Baseline forms submitted: every form row, any grade, as the sheet's Dashboard tab counts.
-- Baseline forms submitted: every form row (a child assessed twice counts twice), as the sheet's
-- Dashboard tab counts "attempted".
--
-- Reaches into silver (int_google_sheet__*): no mart carries the baseline sheet, as on School
-- (prod_do_baseline_chapters reads int_dots__*).
-- Grain: one row per chapter_id.

with by_chapter as (
    select
        school_id                                   as chapter_id,
        count(*) filter (where in_grades_5_to_8)    as children_with_baseline,
        count(*) filter (where in_grades_5_to_8 and cleared_aser)
                                                    as children_cleared_aser,
        sum(forms_submitted)                        as baseline_forms_submitted
    from (
        select *,
            substring(student_grade from '([0-9]+)')::int between 5 and 8 as in_grades_5_to_8
        from {{ ref('int_google_sheet__cci_baseline_children') }}
    ) c
    group by 1
),

grades_5_to_8 as (
    select chapter_id, count(distinct student_id) as children_grades_5_to_8
    from {{ ref('prod_cci_active_children') }}
    where grade between 5 and 8
    group by 1
)

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    s.active_children,
    coalesce(g.children_grades_5_to_8, 0)           as children_grades_5_to_8,
    coalesce(b.children_with_baseline, 0)           as children_with_baseline,
    round(100.0 * coalesce(b.children_with_baseline, 0) / nullif(g.children_grades_5_to_8, 0), 1)
                                                    as pct_children_with_baseline,
    coalesce(b.children_cleared_aser, 0)            as children_cleared_aser,
    coalesce(b.baseline_forms_submitted, 0)         as baseline_forms_submitted
from {{ ref('prod_cci_chapter_summary') }} s
left join by_chapter b on s.chapter_id = b.chapter_id
left join grades_5_to_8 g on s.chapter_id = g.chapter_id
