{{ config(materialized='table') }}

with raw_child_class as (
    select * from {{ source('bubble_staging', 'child_class') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_school_class as (
    select "_id", school_class_id from {{ source('bubble_staging', 'school_class') }}
)
select
    raw.child_class_id::integer             as child_class_id,
    raw.academic_year,
    ref_ch.child_id::integer                as child_id,
    ref_sc.school_class_id::integer         as school_class_id,
    raw.removed                             as removed_boolean,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active
from raw_child_class raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_school_class ref_sc
    on ref_sc."_id" = raw.school_class_id
