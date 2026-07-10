{{ config(materialized='table') }}

with raw_class_section_subject as (
    select * from {{ source('bubble_staging', 'class_section_subject') }}
),
ref_class_section as (
    select "_id", class_section_id from {{ source('bubble_staging', 'class_section') }}
),
ref_subject as (
    select "_id", subject_id from {{ source('bubble_staging', 'subject') }}
)
select
    raw.class_section_subject_id::integer       as class_section_subject_id,
    raw.academic_year,
    ref_cs.class_section_id::integer            as class_section_id,
    ref_sub.subject_id::integer                 as subject_id,
    raw.removed,
    raw.is_active,
    raw."Created_Date"                          as created_date,
    raw."Modified_Date"                         as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section_subject raw
left join ref_class_section ref_cs
    on ref_cs."_id" = raw.class_section_id
left join ref_subject ref_sub
    on ref_sub."_id" = raw.subject_id
