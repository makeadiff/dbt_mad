{{ config(materialized='table') }}

-- DO dashboard (E2): cancelled sessions by reason, one row per chapter per reason (spec §3.7 R3).
-- The reason is the school holiday reason as logged in Bubble, as is (prod_do_circle_sessions);
-- "Reason not logged" when a cancellation has no holiday row. Only reasons that occurred get a row.
--
-- Grain: one row per (chapter_id, cancel_reason).

select
    ch.engine,
    ch.chapter_id,
    ch.chapter_name,
    ch.city,
    ch.co_name,
    ch.cho_name,
    ch.district,
    ch.do_name,
    cs.cancel_reason,
    count(*) as sessions_cancelled
from {{ ref('prod_do_circle_sessions') }} cs
join {{ ref('prod_do_chapter_summary') }} ch
    on cs.chapter_id = ch.chapter_id
where cs.session_group = 'cancelled'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
