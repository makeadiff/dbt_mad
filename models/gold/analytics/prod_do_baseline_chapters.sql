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
-- Forms submitted (DO spec §8, 2026-11-12): every qualifying form, not children, so a child
-- assessed twice counts twice, as the Dots "Assessment Completion" report does. A form is placed
-- on a chapter through its mentoring circle (slot_class_section -> slot -> school), not through the
-- child, so it matches the Dots report (117 / 75 / 75 on 2026-11-12). The AY date filter also drops
-- one numeracy editPublished copy with no created date (a second form for a child who already has
-- one); without it numeracy would read 118 against Dots' 117.
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

forms_submitted as (
    select
        s.school_id::text                                       as chapter_id,
        count(*) filter (where f.form = 'numeracy')             as forms_numeracy_submitted,
        count(*) filter (where f.form = 'literacy')             as forms_literacy_submitted,
        count(*) filter (where f.form = 'life_skills')          as forms_life_skills_submitted
    from {{ ref('int_dots__baseline_submissions') }} f
    join {{ ref('int_bubble__slot_class_section') }} scs
        on f.slot_class_section_id = scs.slot_class_section_id
    join {{ ref('int_bubble__slot') }} s
        on scs.slot_id = s.slot_id
    where f.is_published
      and coalesce(f.assessment_round, 'baseline') = 'baseline'
      and f.created_date >= date '2026-04-01'
    group by 1
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
    round(100.0 * b.children_3_forms / nullif(b.active_children, 0), 1) as pct_baseline_complete,
    coalesce(fs.forms_numeracy_submitted, 0)    as forms_numeracy_submitted,
    coalesce(fs.forms_literacy_submitted, 0)    as forms_literacy_submitted,
    coalesce(fs.forms_life_skills_submitted, 0) as forms_life_skills_submitted
from {{ ref('prod_do_chapter_summary') }} s
left join by_chapter b on s.chapter_id = b.chapter_id
left join forms_submitted fs on s.chapter_id = fs.chapter_id
