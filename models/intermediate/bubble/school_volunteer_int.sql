{{ config(materialized='table') }}

with raw_school_volunteer as (
    select * from {{ source('bubble_staging', 'school_volunteer') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_volunteer as (
    select "_id", user_id from {{ source('bubble_staging', 'user') }}
)
select
    raw.school_volunteer_id::integer        as school_volunteer_id,
    raw.academic_year,
    ref_p.partner_id::integer               as school_id,
    ref_vol.user_id::integer                as volunteer_id,
    raw.is_active,
    raw.removed,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_volunteer raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_volunteer ref_vol
    on ref_vol."_id" = raw.volunteer_id
