{{ config(materialized='table') }}

-- Deduplicates school_holiday records
-- Flow: stg_bubble__school_holiday → int_bubble__school_holiday
-- school_id comes pre-resolved from sessionops_raw (real bigint FK) - no more UUID join
-- needed here now that bronze sources from sessionops_raw. Gives downstream
-- (fct_e2_cancellations) an integer school_id that lines up with int_bubble__class_section.school_id,
-- so planned session dates can be checked against the holiday windows for the same school.
-- Dedupes on school_holiday_id, sessionops's real BigAutoField DB primary key - unlike
-- Bubble's business-key school_holiday_id (which had genuine collisions across different
-- schools' holiday records, see git history), this is guaranteed unique per row.

with joined as (
    select
        raw.school_holiday_id,
        raw.school_id,
        raw.holiday_reason,
        raw.holiday_description,
        raw.remarks,
        raw.start_date,
        raw.end_date,
        raw.is_removed,
        raw.is_active,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__school_holiday') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='school_holiday_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['school_holiday_id']) }} as school_holiday_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    school_holiday_id,
    school_id,
    holiday_reason,
    holiday_description,
    remarks,
    start_date,
    end_date,
    is_removed,
    is_active,
    created_date,
    modified_date
from deduplicated
