{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['retention_id']) }} as retention_key,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_key,
    retention_id,
    user_id,
    retention_year,
    retention_status,
    willing_to_continue,
    preferred_role,
    current_work_node_name,
    preferred_work_node_name,
    retention_sent_date_time
from {{ ref('int_pc_volunteer_retention') }}
