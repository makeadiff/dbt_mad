-- §6.10: coverage is five independent, overlapping states, not a sequence -- do not assert
-- Compliant/Onboarded require Allocated to Class (real, correct counterexamples exist: a
-- volunteer can be compliant/onboarded via PC records without a live Bubble class assignment).
-- Only the two invariants that hold BY CONSTRUCTION from fct_volunteer_pipeline's own boolean
-- logic are tested here -- see assert_sric_funnel_coverage_monotonic.sql for the same check at
-- the prod_sric_funnel (chapter x stage) grain.
--   is_onboarded       = is_compliant AND inducted            => is_onboarded <= is_compliant
--   is_ready_to_mentor  = is_allocated_to_class AND is_compliant AND inducted
--                        => is_ready_to_mentor <= is_allocated_to_class, is_compliant, is_onboarded
select volunteer_id
from {{ ref('fct_volunteer_pipeline') }}
where (is_onboarded and not is_compliant)
   or (is_ready_to_mentor and not is_allocated_to_class)
   or (is_ready_to_mentor and not is_compliant)
   or (is_ready_to_mentor and not is_onboarded)
