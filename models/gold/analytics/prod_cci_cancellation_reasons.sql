{{ config(materialized='table') }}

-- CCI dashboard (E1): cancelled sessions by reason, one row per CCI per reason (spec §3.10).
-- Readable reasons as logged; coded ones as "Reason code N" (prod_cci_cancellations). Only reasons
-- that occurred get a row, so the table is empty until a cancellation is logged.
--
-- Grain: one row per (chapter_id, cancel_reason).

select
    s.chapter_id,
    s.chapter_name,
    s.city,
    s.district,
    s.do_name,
    c.cancel_reason,
    count(*) as sessions_cancelled
from {{ ref('prod_cci_cancellations') }} c
join {{ ref('prod_cci_chapter_summary') }} s
    on c.chapter_id = s.chapter_id
group by 1, 2, 3, 4, 5, 6
