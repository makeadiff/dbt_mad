{{ config(materialized='table') }}

with raw_school_academic_year as (
    select * from {{ source('bubble_staging', 'school_academic_year') }}
),
ref_academic_year as (
    select "_id", academic_year_id from {{ source('bubble_staging', 'academic_year') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
)
select
    raw.school_academic_year_id::integer        as school_academic_year_id,
    ref_ay.academic_year_id::integer            as academic_year_id,
    ref_p.partner_id::integer                   as school_id,
    raw.is_active,
    raw.removed,
    raw."Created_By"                            as created_by,
    raw."Created_Date"                          as created_date,
    raw."Modified_Date"                         as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_academic_year raw
left join ref_academic_year ref_ay
    on ref_ay."_id" = raw.academic_year_id
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
