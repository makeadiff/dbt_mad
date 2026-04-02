{{ config(materialized='table') }}

-- Resolves UUID foreign keys for slot records + deduplicates
-- Flow: stg_bubble__slot → int_bubble__slot
-- Joins: partner (UUID→school_id)

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['raw.slot_id']) }} as slot_sk,
        raw.slot_id,
        raw.slot_name,
        raw.academic_year,
        raw.day_of_week,
        raw.start_time,
        raw.end_time,
        raw.is_recurring,
        partner_map.school_id,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__slot') }} raw
    left join partner_map on raw.school_id = partner_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='slot_id',
        order_by='modified_date desc',
       )
    }}
)

select * from deduplicated
