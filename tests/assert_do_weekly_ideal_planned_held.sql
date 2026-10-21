-- Every weekly row: ideal >= planned >= held >= held by a substitute (spec §3.3b).
select chapter_id, week_start, ideal_sessions, sessions_planned, sessions_held, sessions_held_by_substitute
from {{ ref('prod_do_weekly_sessions') }}
where ideal_sessions < sessions_planned
   or sessions_planned < sessions_held
   or sessions_held < sessions_held_by_substitute
   or sessions_lost_to_late_starts <> ideal_sessions - sessions_planned
