{{ config(materialized='table') }}

-- Deduplicates slot records
-- Flow: stg_bubble__slot → int_bubble__slot
-- school_id and school_academic_year_id come pre-resolved from sessionops_raw (real bigint
-- FKs) - no more UUID join needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['raw.slot_id']) }} as slot_sk,
        raw.slot_id,
        raw.slot_name,
        raw.day_of_week,
        raw.start_time,
        raw.end_time,
        raw.is_recurring,
        raw.school_id,
        raw.school_academic_year_id,
        raw.is_active,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__slot') }} raw
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
