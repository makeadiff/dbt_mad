{{ config(materialized='table') }}

with raw_slot_class_section_volunteer as (
    select * from {{ source('bubble_staging', 'slot_class_section_volunteer') }}
),
ref_slot_class_section as (
    select "_id", slot_class_section_id from {{ source('bubble_staging', 'slot_class_section') }}
),
ref_volunteer as (
    select "_id", user_id from {{ source('bubble_staging', 'user') }}
)
select
    raw.slot_class_section_volunteer_id::integer    as slot_class_section_volunteer_id,
    ref_scs.slot_class_section_id::integer          as slot_class_section_id,
    ref_vol.user_id::integer                        as volunteer_id,
    raw.academic_year,
    raw.is_active,
    raw.removed,
    raw."Created_Date"                              as created_date,
    raw."Modified_Date"                             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot_class_section_volunteer raw
left join ref_slot_class_section ref_scs
    on ref_scs."_id" = raw.slot_class_section_id
left join ref_volunteer ref_vol
    on ref_vol."_id" = raw.volunteer_id
