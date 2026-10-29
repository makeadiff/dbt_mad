{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row (spec §3.6d): when the dashboard's source data last landed, so
-- readers know how fresh the numbers are. Dates are the latest Airbyte extract timestamp (IST) on
-- the DOTS attendance source and on the Bubble child / volunteer sources.

select
    (select max("_airbyte_extracted_at"::timestamp) at time zone 'UTC' at time zone 'Asia/Kolkata'
     from {{ source('dots_raw', 'volunteerAttendance') }})::date    as "Session attendance last synced (DOTS)",
    (select max(t) at time zone 'UTC' at time zone 'Asia/Kolkata'
     from (
        select max("_airbyte_extracted_at"::timestamp) as t from {{ source('sessionops_raw', 'child') }}
        union all
        select max("_airbyte_extracted_at"::timestamp) from {{ source('sessionops_raw', 'school_volunteer') }}
     ) x)::date                                                     as "Volunteers and children last synced (Bubble)"
