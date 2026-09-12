{{ config(materialized='table') }}

-- Deduplicates child_class_section records
-- Flow: stg_bubble__child_class_section → int_bubble__child_class_section
-- child_id and class_section_id come pre-resolved from sessionops_raw (real bigint FKs) -
-- no more UUID joins needed here now that bronze sources from sessionops_raw.
-- Dedupes on child_class_section_id, sessionops's real BigAutoField DB primary key - unlike
-- Bubble's business-key child_class_section_id (which had genuine collisions across different
-- children's records, see git history), this is guaranteed unique per row.

with joined as (
    select
        raw.child_class_section_id,
        raw.child_id,
        raw.class_section_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_class_section') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_class_section_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_class_section_id']) }} as child_class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['child_id']) }} as child_sk,
    {{ dbt_utils.generate_surrogate_key(['class_section_id']) }} as class_section_sk,
    child_class_section_id,
    child_id,
    class_section_id,
    is_removed,
    created_date,
    modified_date
from deduplicated
