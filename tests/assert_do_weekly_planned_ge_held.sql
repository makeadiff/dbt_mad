-- Every weekly row: planned >= held + cancelled, held >= held by a substitute (spec §3.3b, §3.8).
select chapter_id, week_start, sessions_planned, sessions_held, sessions_held_by_substitute, sessions_cancelled
from {{ ref('prod_do_weekly_sessions') }}
where sessions_planned < sessions_held + sessions_cancelled
   or sessions_held < sessions_held_by_substitute
