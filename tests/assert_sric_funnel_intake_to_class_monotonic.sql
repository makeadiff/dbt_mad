-- intake_to_class nests by construction (§ lead funnel, 2026-09-12): each stage's condition is a
-- strict AND-superset of the one before (Applied ⊇ Recruited ⊇ Placed at a School ⊇ Placed in a
-- Class), so volunteers must be non-increasing as stage_order increases. Unlike coverage's
-- monotonicity test (§6.10, which polices an independent-states ASSUMPTION and expects real
-- violations), this block genuinely is a funnel -- a violation here is an actual defect.
with pivoted as (
    select
        max(volunteers) filter (where stage_order = 1) as applied,
        max(volunteers) filter (where stage_order = 2) as recruited,
        max(volunteers) filter (where stage_order = 3) as placed_at_school,
        max(volunteers) filter (where stage_order = 4) as placed_in_class
    from {{ ref('prod_sric_funnel') }}
    where funnel_block = 'intake_to_class'
)
select *
from pivoted
where recruited > applied
   or placed_at_school > recruited
   or placed_in_class > placed_at_school
