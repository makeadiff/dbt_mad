{{ config(materialized='table') }}

with raw_chapter as (
    select * from {{ source('bubble_staging', 'chapter') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
)
select
    raw.chapter_id::integer             as chapter_id,
    raw.academic_year,
    raw.chapter_name,
    raw.city,
    ref_p.partner_id::integer           as school_id,
    raw.state,
    raw.removed,
    raw."Created_Date"                  as created_date,
    raw."Modified_Date"                 as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_chapter raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
