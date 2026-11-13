-- Spec §3.9 W5: per chapter x person, the weekly attendance rows summed over the weeks equal the
-- person's "Since 1 October" row (volunteers: planned, taken; children: held, attended), and every
-- person on one side is on the other. Returns the mismatches.
with vol_weekly as (
    select chapter_id, volunteer_id::text as person_id, sum(sessions_planned) as a, sum(sessions_taken) as b
    from {{ ref('prod_do_volunteer_weekly_attendance') }}
    group by 1, 2
),

vol_term as (
    select chapter_id, volunteer_id::text as person_id, sessions_planned as a, sessions_taken as b
    from {{ ref('prod_do_volunteer_sessions') }}
    where period_type = 'since_1_oct'
),

child_weekly as (
    select chapter_id, child_id::text as person_id, sum(sessions_held) as a, sum(sessions_attended) as b
    from {{ ref('prod_do_child_weekly_attendance') }}
    group by 1, 2
),

child_term as (
    select chapter_id, child_id::text as person_id, sessions_held as a, sessions_attended as b
    from {{ ref('prod_do_child_sessions') }}
    where period_type = 'since_1_oct'
)

select 'volunteer' as side, coalesce(w.chapter_id, t.chapter_id) as chapter_id,
       coalesce(w.person_id, t.person_id) as person_id, w.a as weekly_a, t.a as term_a, w.b as weekly_b, t.b as term_b
from vol_weekly w
full outer join vol_term t on w.chapter_id = t.chapter_id and w.person_id = t.person_id
where w.person_id is null or t.person_id is null or w.a <> t.a or w.b <> t.b

union all

select 'child', coalesce(w.chapter_id, t.chapter_id), coalesce(w.person_id, t.person_id), w.a, t.a, w.b, t.b
from child_weekly w
full outer join child_term t on w.chapter_id = t.chapter_id and w.person_id = t.person_id
where w.person_id is null or t.person_id is null or w.a <> t.a or w.b <> t.b
