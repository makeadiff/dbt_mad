{{ config(materialized='table') }}

-- dim_subject: One row per subject
-- Source: int_bubble__subject (resolves program UUID→ID)

select
    {{ dbt_utils.generate_surrogate_key(['subject_id']) }} as subject_sk,
    subject_id,
    subject_name,
    program_id,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__subject') }}
where is_removed = false
