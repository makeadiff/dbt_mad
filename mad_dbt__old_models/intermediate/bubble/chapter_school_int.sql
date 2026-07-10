{{ config(materialized='table') }}

with raw_chapter_school as (
    select * from {{ source('bubble_staging', 'chapter_school') }}
),
ref_chapter as (
    select "_id", chapter_id from {{ source('bubble_staging', 'chapter') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_co as (
    select "_id", user_id from {{ source('bubble_staging', 'user') }}
)
select
    raw.chapter_school_id::integer      as chapter_school_id,
    ref_ch.chapter_id::integer          as chapter_id,
    raw.academic_year,
    ref_co.user_id::integer             as co_id,
    ref_p.partner_id::integer           as school_id,
    raw.removed,
    raw."Created_Date"                  as created_date,
    raw."Modified_Date"                 as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_chapter_school raw
left join ref_chapter ref_ch
    on ref_ch."_id" = raw.chapter_id
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_co ref_co
    on ref_co."_id" = raw.co_id
