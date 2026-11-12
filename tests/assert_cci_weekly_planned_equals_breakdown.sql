-- Weekly planned, held and cancelled summed by CCI equal the session breakdown summed by CCI and the
-- chapter summary's since-1-October figures, and every CCI with children is in both (spec E1 §7 L1).
with weekly as (
    select chapter_id, sum(sessions_planned) as planned, sum(sessions_held) as held, sum(sessions_cancelled) as cancelled
    from {{ ref('prod_cci_weekly_sessions') }}
    group by 1
),

breakdown as (
    select chapter_id, sum(sessions_planned) as planned, sum(sessions_held) as held, sum(sessions_cancelled) as cancelled
    from {{ ref('prod_cci_session_breakdown') }}
    group by 1
)

select coalesce(w.chapter_id, b.chapter_id, s.chapter_id) as chapter_id
from (select * from {{ ref('prod_cci_chapter_summary') }} where has_children) s
full outer join weekly w on s.chapter_id = w.chapter_id
full outer join breakdown b on coalesce(s.chapter_id, w.chapter_id) = b.chapter_id
where w.chapter_id is null or b.chapter_id is null or s.chapter_id is null
   or w.planned <> b.planned
   or w.held <> b.held
   or w.cancelled <> b.cancelled
   or w.planned <> s.sessions_planned_since_1_oct
   or w.held <> s.sessions_held_since_1_oct
   or w.cancelled <> s.sessions_cancelled_since_1_oct
