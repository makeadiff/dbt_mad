{{ config(materialized='table') }}

-- Deduplicates slot_class_section records
-- Flow: stg_bubble__slot_class_section → int_bubble__slot_class_section
-- slot_id, class_section_id, class_section_subject_id come pre-resolved from sessionops_raw
-- (real bigint FKs) - no more UUID joins needed here now that bronze sources from
-- sessionops_raw.

with joined as (
    select
        raw.slot_class_section_id,
        raw.slot_id,
        raw.class_section_id,
        raw.class_section_subject_id,
        raw.is_removed,
        raw.is_active,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__slot_class_section') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='slot_class_section_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['slot_class_section_id']) }} as slot_class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['slot_id']) }} as slot_sk,
    {{ dbt_utils.generate_surrogate_key(['class_section_id']) }} as class_section_sk,
    slot_class_section_id,
    slot_id,
    class_section_id,
    class_section_subject_id,
    is_removed,
    is_active,
    created_date,
    modified_date
from deduplicated
