{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The small "Last updated" table at the top of every tab on the E1 (CCI)
-- dashboard (639), DO spec §8, 2026-11-12. Dalgo cards can't show dates, so the source is a row
-- with its date as text ("12 Nov 2026"). The weekly-slot-link count stays on the Class structure tab;
-- prod_cci_data_freshness_display stays as it is (chart 3516 reads it until Dalgo swaps).
-- Same timestamp as prod_cci_data_freshness_display: latest xModifiedTimestamp (UTC, shown as the
-- IST date) across the Platform Commons tables the dashboard reads. "Source order" is hidden.
--
-- Grain: one row (Platform Commons).

with latest as (
    select max(t) as t
    from (
        select max("xModifiedTimestamp") as t from {{ source('pc_raw', 'school') }}
        union all
        select max("xModifiedTimestamp") from {{ source('pc_raw', 'student') }}
        union all
        select max("xModifiedTimestamp") from {{ source('pc_raw', 'batchStudent') }}
        union all
        select max("xModifiedTimestamp") from {{ source('pc_raw', 'scLevelBatch') }}
        union all
        select max("xModifiedTimestamp") from {{ source('pc_raw', 'worknodeSlotShift') }}
        union all
        select max("xModifiedTimestamp") from {{ source('pc_raw', 'batchAttendance') }}
    ) x
)

select
    'Platform Commons'                          as "Source",
    trim(to_char((t at time zone 'UTC' at time zone 'Asia/Kolkata')::date, 'FMDD Mon YYYY')) as "Last updated",
    1                                                                               as "Source order"
from latest
