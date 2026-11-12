-- A CCI flagged setup complete has children, 0 outside a mentoring circle and 0 in a circle with no
-- weekly slot; a CCI with children and both at 0 is flagged (spec E1 §7 L1, copying DO §3.8).
-- Minimum volunteers = ceil(active children / 5); circles with and without a weekly slot add up.
select chapter_id, setup_complete, children_not_in_circle, children_in_circle_no_slot
from {{ ref('prod_cci_chapter_summary') }}
where (setup_complete and (active_children = 0 or children_not_in_circle > 0 or children_in_circle_no_slot > 0))
   or (not setup_complete and active_children > 0
       and children_not_in_circle = 0 and children_in_circle_no_slot = 0)
   or volunteers_minimum_to_start <> ceil(active_children / 5.0)
   or circles_with_weekly_slot + circles_no_weekly_slot <> mentoring_circles
