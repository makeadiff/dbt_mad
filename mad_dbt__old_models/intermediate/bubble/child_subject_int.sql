{{ config(materialized='table') }}

with raw_child_subject as (
    select * from {{ source('bubble_staging', 'child_subject') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_class_section_subject as (
    select "_id", class_section_subject_id from {{ source('bubble_staging', 'class_section_subject') }}
)
select
    raw.child_subject_id::integer               as child_subject_id,
    raw.academic_year,
    ref_ch.child_id::integer                    as child_id,
    ref_css.class_section_subject_id::integer   as class_section_subject_id,
    raw.removed,
    raw."Created_Date"                          as created_date,
    raw."Modified_Date"                         as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active
from raw_child_subject raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_class_section_subject ref_css
    on ref_css."_id" = raw.class_section_subject_id
