-- Every CCI weekly row: planned >= held + cancelled, held >= held by a substitute (spec E1 §7 L1).
select chapter_id, week_start, sessions_planned, sessions_held, sessions_held_by_substitute, sessions_cancelled
from {{ ref('prod_cci_weekly_sessions') }}
where sessions_planned < sessions_held + sessions_cancelled
   or sessions_held < sessions_held_by_substitute
