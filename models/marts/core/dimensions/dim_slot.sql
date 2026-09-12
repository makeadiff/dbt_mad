{{ config(materialized='table') }}

-- dim_slot: One row per recurring time slot
-- Source: int_bubble__slot (school_id/school_academic_year_id already resolved bigint FKs)

select
    slot_sk,
    slot_id,
    slot_name,
    day_of_week,
    start_time,
    end_time,
    case
        when start_time is not null and end_time is not null
        then extract(epoch from (end_time - start_time)) / 60
        else null
    end as duration_minutes,
    is_recurring,
    school_id,
    school_academic_year_id,
    is_removed,
    created_date,
    modified_date
from {{ ref('int_bubble__slot') }}
where is_removed = false
