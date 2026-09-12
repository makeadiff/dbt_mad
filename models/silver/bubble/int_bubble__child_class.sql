{{ config(materialized='table') }}

-- Deduplicates child_class records
-- Flow: stg_bubble__child_class → int_bubble__child_class
-- child_id and school_class_id come pre-resolved from sessionops_raw (real bigint FKs) - no
-- more UUID joins needed here now that bronze sources from sessionops_raw.
-- This is the more fundamental child enrollment link: a child must be assigned to a school_class
-- (mandatory at enrollment/edit time) even before, or without ever, being assigned a specific
-- class_section -- so this reaches children that int_bubble__child_class_section misses.
-- Dedupes on child_class_id, sessionops's real BigAutoField DB primary key - unlike Bubble's
-- business-key child_class_id (which had genuine collisions across different children's
-- enrollment records, see git history), this is guaranteed unique per row.

with joined as (
    select
        raw.child_class_id,
        raw.child_id,
        raw.school_class_id,
        raw.is_active,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_class') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_class_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_class_id']) }} as child_class_sk,
    {{ dbt_utils.generate_surrogate_key(['child_id']) }} as child_sk,
    {{ dbt_utils.generate_surrogate_key(['school_class_id']) }} as school_class_sk,
    child_class_id,
    child_id,
    school_class_id,
    is_active,
    is_removed,
    created_date,
    modified_date
from deduplicated
