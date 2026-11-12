{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. The small "Last updated" table at the top of every tab on the E2
-- dashboard (599), DO spec §8, 2026-11-12. Dalgo cards can't show dates, so each source is a row
-- with its date as text ("12 Nov 2026"). prod_do_data_freshness_display stays as it is (chart 3501
-- reads it until Dalgo swaps to this table).
-- Same timestamps as prod_do_data_freshness_display: latest Airbyte extract (IST) on DOTS attendance,
-- and on Bubble children / volunteers. "Source order" sorts the rows and is hidden.
--
-- Grain: one row per source (2 rows).

with dates as (
    select 1 as source_order, 'Session attendance (DOTS)' as source,
        ((select max("_airbyte_extracted_at"::timestamp) from {{ source('dots_raw', 'volunteerAttendance') }})
            at time zone 'UTC' at time zone 'Asia/Kolkata')::date as last_updated
    union all
    select 2, 'Volunteers and children (Bubble)',
        ((select max(t) from (
            select max("_airbyte_extracted_at"::timestamp) as t from {{ source('sessionops_raw', 'child') }}
            union all
            select max("_airbyte_extracted_at"::timestamp) from {{ source('sessionops_raw', 'school_volunteer') }}
        ) x) at time zone 'UTC' at time zone 'Asia/Kolkata')::date
)

select
    source                                                  as "Source",
    trim(to_char(last_updated, 'FMDD Mon YYYY'))            as "Last updated",
    source_order                                            as "Source order"
from dates
