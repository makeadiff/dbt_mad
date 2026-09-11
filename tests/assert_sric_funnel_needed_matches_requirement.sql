-- Needed row lock-step with requirement (2026-09-11): prod_sric_funnel's coverage-block Needed
-- row (stage_order = 0) exists to plot every stage against the chapter's requirement, so it must
-- always equal prod_sric_dashboard_data.volunteers_required -- the one place that target is
-- computed. A drift here would silently mislabel the coverage chart's leftmost bar.
select
    f.chapter_id,
    f.volunteers as needed_row_volunteers,
    d.volunteers_required
from {{ ref('prod_sric_funnel') }} f
inner join {{ ref('prod_sric_dashboard_data') }} d
    on f.chapter_id = d.chapter_id
where f.funnel_block = 'coverage'
  and f.stage_order = 0
  and f.volunteers is distinct from d.volunteers_required
