{{ config(
    materialized = 'table'
) }}

select *
from {{ ref('user_school_chapter_mapping') }}
