{{ config(materialized='table') }}

-- Deduplicates class_section_subject records
-- Flow: stg_bubble__class_section_subject → int_bubble__class_section_subject
-- class_section_id and subject_id come pre-resolved from sessionops_raw (real bigint FKs) -
-- no more UUID joins needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        raw.class_section_subject_id,
        raw.class_section_id,
        raw.subject_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__class_section_subject') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='class_section_subject_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['class_section_subject_id']) }} as class_section_subject_sk,
    {{ dbt_utils.generate_surrogate_key(['class_section_id']) }} as class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['subject_id']) }} as subject_sk,
    class_section_subject_id,
    class_section_id,
    subject_id,
    is_removed,
    created_date,
    modified_date
from deduplicated
