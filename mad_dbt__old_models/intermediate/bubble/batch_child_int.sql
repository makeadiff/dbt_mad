{{ config(materialized='table') }}

with raw_batch_child as (
    select * from {{ source('bubble_staging', 'batch_child') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_school_academic_year as (
    select "_id", school_academic_year_id from {{ source('bubble_staging', 'school_academic_year') }}
)
select
    raw.batch_child_id::integer                     as batch_child_id,
    raw.academic_year,
    ref_ch.child_id::integer                        as child_id,
    ref_p.partner_id::integer                       as school_id,
    raw.removed                                     as removed_boolean,
    raw."Created_Date"                              as created_date,
    raw."Modified_Date"                             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta",
    raw.is_active,
    ref_say.school_academic_year_id::integer        as school_academic_year_id
from raw_batch_child raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_school_academic_year ref_say
    on ref_say."_id" = raw.school_academic_year_id
