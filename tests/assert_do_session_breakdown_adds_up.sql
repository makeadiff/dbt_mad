-- The groups add up to planned on every chapter-week (held + cancelled + volunteer absent + no
-- volunteer assigned, with held = original + substitute), planned minus cancelled equals the weekly
-- model's "Sessions planned", and held matches (spec DO §3.8 ruling 1, §3.3b). Returns a row only
-- when something differs.
select b.chapter_id, b.week_start
from {{ ref('prod_do_session_breakdown') }} b
left join {{ ref('prod_do_weekly_sessions') }} w
    on b.chapter_id = w.chapter_id
   and b.week_start = w.week_start
where b.sessions_held + b.sessions_cancelled + b.sessions_volunteer_absent
        + b.sessions_no_volunteer_assigned <> b.sessions_planned
   or b.sessions_held_by_original + b.sessions_held_by_substitute <> b.sessions_held
   or b.sessions_planned - b.sessions_cancelled <> w.sessions_planned
   or b.sessions_held <> w.sessions_held
   or b.month_label <> w.month_label
