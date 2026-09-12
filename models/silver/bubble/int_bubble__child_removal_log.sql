{{ config(materialized='table') }}

-- Deduplicates child removal log records
-- Flow: stg_bubble__child_removal_log → int_bubble__child_removal_log
-- child_id, school_id, and co_id come pre-resolved from sessionops_raw (real bigint FKs) -
-- no more UUID joins needed here now that bronze sources from sessionops_raw.

with joined as (
    select
        raw.child_removal_log_id,
        raw.child_id,
        raw.school_id,
        raw.co_id,
        raw.other_details,
        raw.removal_reason,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_removal_log') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='child_removal_log_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['child_removal_log_id']) }} as child_removal_log_sk,
    {{ dbt_utils.generate_surrogate_key(['child_id']) }} as child_sk,
    {{ dbt_utils.generate_surrogate_key(['school_id']) }} as school_sk,
    {{ dbt_utils.generate_surrogate_key(['co_id']) }} as co_sk,
    child_removal_log_id,
    child_id,
    school_id,
    co_id,
    other_details,
    removal_reason,
    is_removed,
    created_date,
    modified_date
from deduplicated
