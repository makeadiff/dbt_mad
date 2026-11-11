-- Weekly planned, held and cancelled summed by chapter equal the session breakdown summed by chapter
-- (planned includes cancelled dates everywhere, ruled 2026-11-11), and both cover every chapter.
with weekly as (
    select chapter_id, sum(sessions_planned) as planned, sum(sessions_held) as held, sum(sessions_cancelled) as cancelled
    from {{ ref('prod_do_weekly_sessions') }}
    group by 1
),

breakdown as (
    select chapter_id, sum(sessions_planned) as planned, sum(sessions_held) as held, sum(sessions_cancelled) as cancelled
    from {{ ref('prod_do_session_breakdown') }}
    group by 1
)

select coalesce(w.chapter_id, b.chapter_id) as chapter_id
from weekly w
full outer join breakdown b on w.chapter_id = b.chapter_id
where w.chapter_id is null or b.chapter_id is null
   or w.planned <> b.planned
   or w.held <> b.held
   or w.cancelled <> b.cancelled
