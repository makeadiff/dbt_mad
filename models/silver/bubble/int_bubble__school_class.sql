{{ config(materialized='table') }}

-- Deduplicates school_class records
-- Flow: stg_bubble__school_class → int_bubble__school_class
-- class_id, school_id, and school_academic_year_id come pre-resolved from sessionops_raw
-- (real bigint FKs) - no more UUID joins needed here now that bronze sources from
-- sessionops_raw.

with joined as (
    select
        raw.school_class_id,
        raw.class_id,
        raw.school_id,
        raw.school_academic_year_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_class') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_class_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    {{ dbt_utils.generate_surrogate_key(['class_id']) }} as class_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    school_class_id,
    class_id,
    school_id,
    school_academic_year_id,
    is_removed,
    created_date,
    modified_date
from deduplicated
