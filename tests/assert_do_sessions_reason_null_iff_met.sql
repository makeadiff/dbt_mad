-- The sessions-commenced reason is empty exactly when the chapter meets Metric 2.
select chapter_id
from {{ ref('prod_do_chapter_summary') }}
where (sessions_commenced_not_met_reason is null) <> sessions_commenced_met
