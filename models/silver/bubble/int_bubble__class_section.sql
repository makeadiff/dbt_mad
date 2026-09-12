{{ config(materialized='table') }}

-- Deduplicates class_section records
-- Flow: stg_bubble__class_section → int_bubble__class_section
-- school_class_id, school_id, and school_academic_year_id come pre-resolved from
-- sessionops_raw (real bigint FKs) - no more UUID joins needed here now that bronze
-- sources from sessionops_raw. school_academic_year_id is now a direct column on
-- class_section itself (SESSIONOPS_SCHEMA_CHANGE_PLAN item 2), not derived from school_class.

with joined as (
    select
        raw.class_section_id,
        raw.section_name,
        raw.section_display_name,
        raw.is_removed,
        raw.is_active,
        raw.school_class_id,
        raw.school_id,
        raw.school_academic_year_id,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__class_section') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='class_section_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['class_section_id']) }} as class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    class_section_id,
    section_name,
    section_display_name,
    is_removed,
    is_active,
    school_class_id,
    school_id,
    school_academic_year_id,
    created_date,
    modified_date
from deduplicated
