{{ config(materialized='table') }}

with raw_child as (
    select * from {{ source('bubble_staging', 'child') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_class as (
    select "_id", class_id from {{ source('bubble_staging', 'class') }}
),
ref_school_class as (
    select "_id", school_class_id from {{ source('bubble_staging', 'school_class') }}
)
select
    raw.child_id::integer                   as child_id,
    raw.first_name,
    raw.last_name,
    raw.gender,
    raw.dob,
    raw.city,
    raw.date_of_enrollment,
    raw.mother_tounge,
    raw."Age"                               as age,
    raw.is_active,
    raw.removed,
    ref_c.class_id::integer                 as class_id,
    ref_sc.school_class_id::integer         as school_class_id,
    ref_p.partner_id::integer               as school_id,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_class ref_c
    on ref_c."_id" = raw.class_id
left join ref_school_class ref_sc
    on ref_sc."_id" = raw.school_class_id
