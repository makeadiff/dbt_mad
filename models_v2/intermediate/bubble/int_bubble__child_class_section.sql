{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_class_section records + deduplicates
-- Flow: stg_bubble__child_class_section → int_bubble__child_class_section
-- Joins: child (UUID→child_id), class_section (UUID→class_section_id)

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
class_section_map as (
    select _id as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),

joined as (
    select
        raw.child_class_section_id,
        raw.academic_year,
        child_map.child_id,
        class_section_map.class_section_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_class_section') }} raw
    left join child_map on raw.child_id = child_map.uuid
    left join class_section_map on raw.class_section_id = class_section_map.uuid
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
    academic_year,
    is_removed,
    created_date,
    modified_date
from deduplicated
