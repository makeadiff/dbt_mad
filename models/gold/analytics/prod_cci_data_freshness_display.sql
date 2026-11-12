{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row (spec E1 §3.8): when Platform Commons data last landed, and how
-- many mentoring circles have a weekly slot day. Since layout v4 (spec §7 L1) the slot day comes from
-- Platform Commons' class ops data (prod_cci_mentoring_circles.has_weekly_slot), not the pc_raw
-- bridge; circles without one have no planned sessions. Last synced is the latest xModifiedTimestamp
-- (UTC, shown as the IST date) across the Platform Commons tables the dashboard reads; the volunteer
-- list table carries no such column, so it isn't in the max.

select
    (select max(t) at time zone 'UTC' at time zone 'Asia/Kolkata'
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
     ) x)::date                                                         as "Platform Commons last synced",
    count(*) filter (where has_weekly_slot)::text || ' of ' || count(*)::text
                                                                        as "Mentoring circles with a weekly slot linked"
from {{ ref('prod_cci_mentoring_circles') }}
