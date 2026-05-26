{{ config(materialized='table') }}

with raw_slot_class_section as (
    select * from {{ source('bubble_staging', 'slot_class_section') }}
),
ref_slot as (
    select "_id", slot_id from {{ source('bubble_staging', 'slot') }}
),
ref_class_section as (
    select "_id", class_section_id from {{ source('bubble_staging', 'class_section') }}
),
ref_class_section_subject as (
    select "_id", class_section_subject_id from {{ source('bubble_staging', 'class_section_subject') }}
)
select
    raw.slot_class_section_id::integer              as slot_class_section_id,
    ref_s.slot_id::integer                          as slot_id,
    ref_cs.class_section_id::integer                as class_section_id,
    ref_css.class_section_subject_id::integer       as class_section_subject_id,
    raw.academic_year,
    raw.is_active,
    raw.removed,
    raw."Created_Date"                              as created_date,
    raw."Modified_Date"                             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_slot_class_section raw
left join ref_slot ref_s
    on ref_s."_id" = raw.slot_id
left join ref_class_section ref_cs
    on ref_cs."_id" = raw.class_section_id
left join ref_class_section_subject ref_css
    on ref_css."_id" = raw.class_section_subject_id
