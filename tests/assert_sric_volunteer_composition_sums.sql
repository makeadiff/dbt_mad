-- volunteers_new_this_year + volunteers_continuing must sum exactly to
-- volunteers_allocated_to_school -- they're a full, non-overlapping partition of it
-- (is_recruited_new vs NOT is_recruited_new over the same is_allocated_to_school population).
select
    chapter_id,
    volunteers_allocated_to_school,
    volunteers_new_this_year,
    volunteers_continuing
from {{ ref('prod_sric_dashboard_data') }}
where volunteers_new_this_year + volunteers_continuing <> volunteers_allocated_to_school
