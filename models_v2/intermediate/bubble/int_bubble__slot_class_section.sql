{{ config(materialized='table') }}

-- Resolves UUID foreign keys for slot_class_section records + deduplicates
-- Flow: stg_bubble__slot_class_section → int_bubble__slot_class_section
-- Joins: slot, class_section, class_section_subject (UUID→IDs)

with slot_map as (
    select _id as uuid, slot_id
    from {{ ref('stg_bubble__slot') }}
),
class_section_map as (
    select _id as uuid, class_section_id
    from {{ ref('stg_bubble__class_section') }}
),
class_section_subject_map as (
    select _id as uuid, class_section_subject_id
    from {{ ref('stg_bubble__class_section_subject') }}
),

joined as (
    select
        raw.slot_class_section_id,
        slot_map.slot_id,
        class_section_map.class_section_id,
        class_section_subject_map.class_section_subject_id,
        raw.academic_year,
        raw.is_removed,
        raw.is_active,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__slot_class_section') }} raw
    left join slot_map on raw.slot_id = slot_map.uuid
    left join class_section_map on raw.class_section_id = class_section_map.uuid
    left join class_section_subject_map on raw.class_section_subject_id = class_section_subject_map.uuid
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
    academic_year,
    is_removed,
    is_active,
    created_date,
    modified_date
from deduplicated
