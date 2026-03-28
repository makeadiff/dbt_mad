{{ config(materialized='table') }}

-- dim_chapter: One row per chapter
-- Flow: stg_bubble__chapter → dim_chapter (joins partner for city/state backup)
-- Chapter staging already has city and state directly

select
    raw.chapter_id,
    raw.chapter_name,
    raw.academic_year,
    raw.city,
    raw.state,
    raw.created_date,
    raw.modified_date
from {{ ref('stg_bubble__chapter') }} raw
