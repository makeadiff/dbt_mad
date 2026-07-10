{{ config(materialized='table') }}

with raw_slot as (
    select * from {{ source('bubble_staging', 'slot') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_school_academic_year as (
    select "_id", school_academic_year_id from {{ source('bubble_staging', 'school_academic_year') }}
)
select
    raw.slot_id::integer                            as slot_id,
    raw.slot_name,
    raw.academic_year,
    raw.day_of_week,
    raw.start_time,
    raw.end_time,
    raw.reccuring,
    ref_p.partner_id::integer                       as school_id,
    raw.is_active,
    raw.removed,
    ref_say.school_academic_year_id::integer        as school_academic_year_id,
    raw."Created_Date"                              as created_date,
    raw."Modified_Date"                             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_school_academic_year ref_say
    on ref_say."_id" = raw.school_academic_year_id
