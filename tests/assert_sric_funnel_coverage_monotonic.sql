-- §6.10, SIGNED 2026-08-26: Coverage is five STATES, not a funnel -- Allocated to School,
-- Allocated to Class, Compliant, Onboarded, Active in Class come from independent systems that do
-- not nest by stage_order. Confirmed: is_compliant reads PC applicant-policy records,
-- is_allocated_to_class reads Bubble slot assignments -- a volunteer can be compliant without a
-- live class assignment, so Compliant > Allocated to Class is a real, correct state, not a bug.
-- Per §6.8, is_allocated_to_school is also undercounted (backfill drops null-school rows with no
-- class assignment), so Allocated to Class > Allocated to School is possible too. Do NOT assert
-- Compliant <= Allocated to Class, or any other stage_order-based ordering -- that would be
-- forcing a sequence onto data that was never sequential.
--
-- Only two invariants hold by construction (from fct_volunteer_pipeline's own boolean logic, not
-- from any assumed ordering):
--   Onboarded <= Compliant             (is_onboarded = is_compliant AND inducted)
--   Active in Class <= Allocated to Class  (is_active_in_class = is_allocated_to_class AND
--                                            is_compliant AND inducted)
--   Active in Class <= Compliant
--   Active in Class <= Onboarded
with pivoted as (
    select
        chapter_id,
        volunteer_source,
        max(volunteers) filter (where stage_name = 'Allocated to Class') as allocated_to_class,
        max(volunteers) filter (where stage_name = 'Compliant') as compliant,
        max(volunteers) filter (where stage_name = 'Onboarded') as onboarded,
        max(volunteers) filter (where stage_name = 'Active in Class') as active_in_class
    from {{ ref('prod_sric_funnel') }}
    where funnel_block = 'coverage'
    group by chapter_id, volunteer_source
)
select *
from pivoted
where onboarded > compliant
   or active_in_class > allocated_to_class
   or active_in_class > compliant
   or active_in_class > onboarded
