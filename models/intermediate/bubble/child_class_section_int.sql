{{ config(materialized='table') }}

with raw_child_class_section as (
    select * from {{ source('bubble_staging', 'child_class_section') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_class_section as (
    select "_id", class_section_id from {{ source('bubble_staging', 'class_section') }}
)
select
    raw.child_class_section_id::integer         as child_class_section_id,
    raw.academic_year,
    ref_ch.child_id::integer                    as child_id,
    ref_cs.class_section_id::integer            as class_section_id,
    raw.removed                                 as removed_boolean,
    raw."Created_Date"                          as created_date,
    raw."Modified_Date"                         as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active
from raw_child_class_section raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_class_section ref_cs
    on ref_cs."_id" = raw.class_section_id
