-- CCI baseline (E1 spec §7 B3-B5): cleared ASER is never more than children with a baseline, and
-- children never more than forms. The district rows (All districts included) and the national row
-- equal the sum of the CCIs, and active children match prod_cci_chapter_summary, so the Baseline
-- tab and the other tabs show the same children. Foundations only (2026-11-14): children with a
-- baseline never pass the CCI's Foundations children, nor the sheet count.
with chapters as (
    select
        sum(children_with_baseline)     as children,
        sum(children_cleared_aser)      as cleared,
        sum(baseline_forms_submitted)   as forms
    from {{ ref('prod_cci_baseline_chapters') }}
),

checks as (
    select 'chapter ' || b.chapter_id as row_name
    from {{ ref('prod_cci_baseline_chapters') }} b
    join {{ ref('prod_cci_chapter_summary') }} s on b.chapter_id = s.chapter_id
    where b.active_children <> s.active_children
       or b.children_foundations <> s.children_foundations
       or b.children_with_baseline > b.children_foundations
       or b.children_with_baseline > b.children_with_baseline_on_sheet
       or b.children_cleared_aser > b.children_with_baseline
       or b.children_with_baseline > b.baseline_forms_submitted

    union all
    select 'district sum'
    from {{ ref('prod_cci_district_summary') }} d, chapters c
    where d.district <> 'All districts'
    having sum(d.children_with_baseline) <> max(c.children)
        or sum(d.children_cleared_aser) <> max(c.cleared)
        or sum(d.baseline_forms_submitted) <> max(c.forms)

    union all
    select 'All districts'
    from {{ ref('prod_cci_district_summary') }} d, chapters c
    where d.district = 'All districts'
      and (d.children_with_baseline <> c.children
        or d.children_cleared_aser <> c.cleared
        or d.baseline_forms_submitted <> c.forms)

    union all
    select 'national'
    from {{ ref('prod_cci_national_display') }} n, chapters c
    where n."Children with a baseline" <> c.children
       or n."Children who cleared ASER" <> c.cleared
       or n."Baseline forms submitted" <> c.forms
)

select * from checks
