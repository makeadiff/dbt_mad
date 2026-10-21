{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['snapshot_date', 'chapter_id'],
    full_refresh=false,
    enabled=(target.name == 'prod')
) }}

-- DO dashboard (E2): daily history of prod_do_chapter_summary for the weekly trend. Each run
-- appends today's rows; a re-run on the same day replaces that day's rows.
--
-- Prod only (disabled on every other target). full_refresh=false so a --full-refresh can never
-- wipe the history. snapshot_date is the IST date: the nightly job runs at 01:36 IST, when the
-- UTC date is still the previous day.
--
-- Grain: one row per (snapshot_date, chapter_id).

select
    (now() at time zone 'Asia/Kolkata')::date as snapshot_date,
    *
from {{ ref('prod_do_chapter_summary') }}
