-- Consistent volunteers can't exceed volunteers with sessions planned, and taken sessions can't
-- exceed planned ones.
select chapter_id, period_type, period_start
from {{ ref('prod_do_volunteer_consistency') }}
where volunteers_consistent > volunteers_with_planned_sessions
   or sessions_taken > planned_sessions
