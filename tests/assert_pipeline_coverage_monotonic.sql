-- §11: funnel stage counts must be monotonically non-increasing left to right, WITHIN the
-- Coverage block only (Mapped -> Allocated -> Onboarded -> Active in class) -- §12c forbids
-- comparing Intake and Coverage against each other, since they're different populations.
select volunteer_id
from {{ ref('fct_volunteer_pipeline') }}
where (is_allocated and not is_mapped)
   or (is_onboarded and not is_allocated)
   or (is_active_in_class and not is_onboarded)
