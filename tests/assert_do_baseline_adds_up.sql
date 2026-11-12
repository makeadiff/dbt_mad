-- Baseline (spec §3.4): the 0 / 1 / 2 / 3-form split adds up to active children, and complete
-- (all three forms) is never more than any one form's count. Checked per chapter, on every
-- district row (All districts included) and on the national row. Active children also match
-- prod_do_chapter_summary, so the Baseline tab and the other tabs show the same children.
with chapters as (
    select 'chapter ' || b.chapter_id as row_name, b.active_children, s.active_children as summary_children,
        children_numeracy_done, children_literacy_done, children_life_skills_done,
        children_0_forms, children_1_form, children_2_forms, children_3_forms, children_baseline_complete
    from {{ ref('prod_do_baseline_chapters') }} b
    join {{ ref('prod_do_chapter_summary') }} s on b.chapter_id = s.chapter_id
),

districts as (
    select 'district ' || district, active_children, active_children,
        children_numeracy_done, children_literacy_done, children_life_skills_done,
        children_0_forms, children_1_form, children_2_forms, children_3_forms, children_baseline_complete
    from {{ ref('prod_do_district_summary') }}
),

national as (
    select 'national', active_children, active_children,
        children_numeracy_done, children_literacy_done, children_life_skills_done,
        children_0_forms, children_1_form, children_2_forms, children_3_forms, children_baseline_complete
    from {{ ref('prod_do_national_kpis') }}
),

all_rows as (
    select * from chapters
    union all select * from districts
    union all select * from national
)

select row_name, active_children, summary_children, children_0_forms, children_1_form,
    children_2_forms, children_3_forms, children_baseline_complete
from all_rows
where children_0_forms + children_1_form + children_2_forms + children_3_forms <> active_children
   or active_children <> summary_children
   or children_baseline_complete <> children_3_forms
   or children_baseline_complete > children_numeracy_done
   or children_baseline_complete > children_literacy_done
   or children_baseline_complete > children_life_skills_done
