-- §4 ratio rule: no ratio metric may exceed 100%. Placeholder monitor for the ratios already
-- computable from this model's raw counts (recruited vs target, signed vs recruited) -- asserted
-- by eye until §4's ratio metrics are built in the BI layer (phase 2).
select
    chapter_id,
    chapter,
    volunteers_compliant,
    volunteers_allocated_to_school
from {{ ref('prod_sric_dashboard_data') }}
where volunteers_allocated_to_school > 0
  and volunteers_compliant > volunteers_allocated_to_school
