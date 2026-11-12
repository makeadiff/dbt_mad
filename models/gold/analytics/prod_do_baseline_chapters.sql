{{ config(materialized='table') }}

-- DO dashboard (E2), Metric 4: baseline complete (target 80% of children), spec §3.4, rulings
-- B1-B5 signed 2026-11-12. One row per chapter, every chapter in prod_do_chapter_summary.
--
-- A form counts for a child when it is published (published / editPublished, B3), its round is
-- baseline or blank (B2; midlines and endline don't count) and it was submitted in AY 2026-27.
-- A child counts once per form however many qualifying forms they have. The child is matched to a
-- chapter through prod_do_active_children (child_id), not through the form's mentoring circle, so
-- only active children count and the denominator is all active children (§8, 2026-11-12: not
-- narrowed to chapters using DOTS). Complete = all three forms done (B1).
--
-- Counts children, not forms, so it reads a little below the Dots "Assessment Completion" report
-- (a few children are assessed twice).
--
-- Source: int_dots__baseline_submissions (PR #117).
-- Grain: one row per chapter_id.

with forms_done as (
    select distinct child_id::bigint as child_id, form
    from {{ ref('int_dots__baseline_submissions') }}
    where is_published
      and coalesce(assessment_round, 'baseline') = 'baseline'
      and child_id is not null
      and created_date >= date '2026-04-01'
),

children as (
    select
        ac.chapter_id,
        ac.child_id,
        bool_or(f.form = 'numeracy')    as has_numeracy,
        bool_or(f.form = 'literacy')    as has_literacy,
        bool_or(f.form = 'life_skills') as has_life_skills,
        count(distinct f.form)          as forms_done
    from {{ ref('prod_do_active_children') }} ac
    left join forms_done f on ac.child_id = f.child_id
    group by 1, 2
),

by_chapter as (
    select
        chapter_id,
        count(*)                                            as active_children,
        count(*) filter (where has_numeracy)                as children_numeracy_done,
        count(*) filter (where has_literacy)                as children_literacy_done,
        count(*) filter (where has_life_skills)             as children_life_skills_done,
        count(*) filter (where forms_done = 0)              as children_0_forms,
        count(*) filter (where forms_done = 1)              as children_1_form,
        count(*) filter (where forms_done = 2)              as children_2_forms,
        count(*) filter (where forms_done = 3)              as children_3_forms
    from children
    group by 1
)

select
    s.engine,
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.co_name,
    s.cho_name,
    s.district,
    s.do_name,
    coalesce(b.active_children, 0)              as active_children,
    coalesce(b.children_numeracy_done, 0)       as children_numeracy_done,
    coalesce(b.children_literacy_done, 0)       as children_literacy_done,
    coalesce(b.children_life_skills_done, 0)    as children_life_skills_done,
    coalesce(b.children_0_forms, 0)             as children_0_forms,
    coalesce(b.children_1_form, 0)              as children_1_form,
    coalesce(b.children_2_forms, 0)             as children_2_forms,
    coalesce(b.children_3_forms, 0)             as children_3_forms,
    coalesce(b.children_3_forms, 0)             as children_baseline_complete,
    round(100.0 * b.children_3_forms / nullif(b.active_children, 0), 1) as pct_baseline_complete
from {{ ref('prod_do_chapter_summary') }} s
left join by_chapter b on s.chapter_id = b.chapter_id
