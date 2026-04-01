{{ config(materialized='table') }}

-- dim_chapter: One row per chapter
-- Flow: stg_bubble__chapter → dim_chapter (joins partner for city/state backup)
-- Chapter staging already has city and state directly

select
    {{ dbt_utils.generate_surrogate_key(['chapter_id']) }} as chapter_sk,
    chapter_id,
    chapter_name,
    academic_year,
    city,
    state,
    created_date,
    modified_date
from {{ ref('stg_bubble__chapter') }}
