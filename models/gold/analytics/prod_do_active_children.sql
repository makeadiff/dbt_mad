{{ config(materialized='table') }}

-- DO dashboard (E2): every active child per chapter for AY 2026-27. Shared base for
-- prod_do_chapter_summary (child cover) and prod_do_mentoring_circles (which circles count), so the
-- active-child rule lives in one place. Not read by Dalgo.
--
-- Active child: child active and not removed; child_class not removed, on a not-removed
-- school_class in the chapter's 2026-27 year; no not-removed child_removal_log row dated after
-- that enrolment. Verified against the frontend at 427 / 604 / 56 / 554.
--
-- Grain: one row per (chapter_id, child_id).

with school_years as (
    select say.school_academic_year_id, say.school_id::text as chapter_id
    from {{ ref('int_bubble__school_academic_year') }} say
    join {{ ref('int_bubble__academic_year') }} ay
        on say.academic_year_id = ay.academic_year_id
    where ay.label = '2026-2027'
),

latest_removal as (
    select child_id, max(created_date) as last_removal_date
    from {{ ref('int_bubble__child_removal_log') }}
    where not is_removed
    group by 1
)

select distinct sy.chapter_id, cc.child_id
from {{ ref('int_bubble__child_class') }} cc
join {{ ref('int_bubble__school_class') }} sc
    on cc.school_class_id = sc.school_class_id
join school_years sy
    on sc.school_academic_year_id = sy.school_academic_year_id
join {{ ref('int_bubble__children') }} c
    on cc.child_id = c.child_id
left join latest_removal rl
    on cc.child_id = rl.child_id
where c.is_active and not c.is_removed
  and not cc.is_removed
  and not sc.is_removed
  and (rl.last_removal_date is null or rl.last_removal_date <= cc.created_date)
