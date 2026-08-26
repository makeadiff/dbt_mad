-- §11: funnel stage counts must be monotonically non-increasing left to right, WITHIN the
-- Coverage block only (Allocated to School -> Allocated to Class -> Onboarded -> Active in class)
-- -- §12c forbids comparing Intake and Coverage against each other, since they're different
-- populations.
select volunteer_id
from {{ ref('fct_volunteer_pipeline') }}
where (is_allocated_to_class and not is_allocated_to_school)
   or (is_onboarded and not is_allocated_to_class)
   or (is_active_in_class and not is_onboarded)
