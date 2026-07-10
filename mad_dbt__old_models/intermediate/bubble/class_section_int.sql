{{ config(materialized='table') }}

with raw_class_section as (
    select * from {{ source('bubble_staging', 'class_section') }}
),
ref_school_class as (
    select "_id", school_class_id from {{ source('bubble_staging', 'school_class') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
)
select
    raw.class_section_id::integer           as class_section_id,
    raw.academic_year,
    raw.section_name,
    raw.section_code,
    raw.removed,
    raw.is_active,
    ref_sc.school_class_id::integer         as school_class_id,
    ref_p.partner_id::integer               as school_id,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_class_section raw
left join ref_school_class ref_sc
    on ref_sc."_id" = raw.school_class_id
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
