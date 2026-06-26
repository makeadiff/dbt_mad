{{ config(materialized='table') }}

-- Resolves UUID foreign keys for child removal logs + deduplicates
-- Flow: stg_bubble__child_removal_log → int_bubble__child_removal_log

with child_map as (
    select _id as uuid, child_id
    from {{ ref('stg_bubble__children') }}
),
partner_map as (
    select partner_id as uuid, partner_id1 as school_id
    from {{ ref('stg_bubble__partner') }}
),
user_map as (
    select user_id as uuid, user_id_number as co_id
    from {{ ref('stg_bubble__user') }}
),

joined as (
    select
        raw.child_removal_log_id,
        child_map.child_id,
        partner_map.school_id,
        user_map.co_id,
        raw.other_details,
        raw.removal_reason,
        raw.is_removed,
        raw.created_date,
        raw.modified_date
    from {{ ref('stg_bubble__child_removal_log') }} raw
    left join child_map on raw.child_id = child_map.uuid
    left join partner_map on raw.school_id = partner_map.uuid
    left join user_map on raw.co_id = user_map.uuid
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
