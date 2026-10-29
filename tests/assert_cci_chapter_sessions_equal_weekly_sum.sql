-- Sessions held since 1 October on the chapter table equal the sum of the weekly rows (spec E1 §3.5).
select s.chapter_id
from {{ ref('prod_cci_chapter_summary') }} s
left join (
    select chapter_id, sum(sessions_held) as sessions_held
    from {{ ref('prod_cci_weekly_sessions') }}
    group by 1
) w on s.chapter_id = w.chapter_id
where s.has_children
  and s.sessions_held_since_1_oct <> coalesce(w.sessions_held, 0)
