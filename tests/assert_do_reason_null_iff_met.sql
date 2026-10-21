-- The not-met reason is empty exactly when the chapter meets the 1:5 requirement.
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where (volunteer_requirement_not_met_reason is null) <> volunteer_requirement_met_1to5
