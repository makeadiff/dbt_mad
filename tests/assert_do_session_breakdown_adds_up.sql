-- The five groups add up to planned on every chapter-week, and planned minus cancelled equals the
-- weekly model's "Sessions planned" (spec DO §3.7 R2, §3.3b). Returns a row only when something differs.
select b.chapter_id, b.week_start
from {{ ref('prod_do_session_breakdown') }} b
left join {{ ref('prod_do_weekly_sessions') }} w
    on b.chapter_id = w.chapter_id
   and b.week_start = w.week_start
where b.sessions_cancelled + b.sessions_held_by_original + b.sessions_held_by_substitute
        + b.sessions_volunteer_absent + b.sessions_no_attendance_logged <> b.sessions_planned
   or b.sessions_planned - b.sessions_cancelled <> w.sessions_planned
   or b.sessions_held <> w.sessions_held
