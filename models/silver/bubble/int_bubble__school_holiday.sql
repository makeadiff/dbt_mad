{{ config(materialized='table') }}

-- Resolves UUID foreign keys for school_holiday records + deduplicates
-- Flow: stg_bubble__school_holiday → int_bubble__school_holiday
-- Joins: partner (UUID→school_id)
-- Gives downstream (fct_e2_cancellations) an integer school_id that lines up with
-- int_bubble__class_section.school_id, so planned session dates can be checked
-- against the holiday windows for the same school.
-- Deduplicates on Bubble's raw "_id", not "school_holiday_id" -- unlike every other
-- int_bubble__* entity id, school_holiday_id is not a reliable global unique key here (observed
-- two genuinely different holiday records, for two different schools, both stamped
-- school_holiday_id=1); partitioning on it silently dropped one of the two as a "duplicate".

with partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),

joined as (
    select
        raw."_id" as school_holiday_uid,
        raw.school_holiday_id,
        partner_map.school_id,
        raw.holiday_reason,
        raw.holiday_description,
        raw.start_date,
        raw.end_date,
        raw.is_removed,
        raw.is_active,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_holiday') }} raw
    left join partner_map on raw.school_id = partner_map.uuid
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_holiday_uid',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_holiday_uid']) }} as school_holiday_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    school_holiday_id,
    school_id,
    holiday_reason,
    holiday_description,
    start_date,
    end_date,
    is_removed,
    is_active,
    created_date,
    modified_date
from deduplicated
