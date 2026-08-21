{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child_class_section records + deduplicates
-- Flow: stg_bubble__child_class_section → int_bubble__child_class_section
-- Joins: child (UUID→child_id), class_section (UUID→class_section_id)
-- Deduplicates on Bubble's raw "_id", not "child_class_section_id" -- like school_holiday_id in
-- int_bubble__school_holiday, child_class_section_id is not a reliable global unique key: found 41
-- groups (87 raw rows) where the same child_class_section_id is shared by genuinely different
-- records for different children assigned to the same class_section, same modified_date. Partitioning
-- on it silently dropped one child's real assignment as a "duplicate" of the other's -- e.g.
-- child_class_section_id=7211 covers both child 4724 and child 4725, both assigned to class_section
-- 1564; only 4725 survived the old dedup, making 4724 look like they had no section assignment at all.

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
        raw."_id" as child_class_section_uid,
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
        partition_by='child_class_section_uid',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_class_section_uid']) }} as child_class_section_sk,
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
