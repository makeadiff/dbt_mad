{{ config(materialized='table') }}

with raw_co_chapter as (
    select * from {{ source('bubble_staging', 'co_chapter') }}
),
ref_chapter as (
    select "_id", chapter_id from {{ source('bubble_staging', 'chapter') }}
),
ref_co as (
    select "_id", user_id from {{ source('bubble_staging', 'user') }}
)
select
    raw.co_chapter_id::integer      as co_chapter_id,
    ref_ch.chapter_id::integer      as chapter_id,
    raw.academic_year,
    ref_co.user_id::integer         as co_id,
    raw.start_date,
    raw.end_date,
    raw.is_active,
    raw.removed,
    raw."Created_Date"              as created_date,
    raw."Modified_Date"             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_co_chapter raw
left join ref_chapter ref_ch
    on ref_ch."_id" = raw.chapter_id
left join ref_co ref_co
    on ref_co."_id" = raw.co_id
