{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['applicant_id']) }} as fellow_application_key,
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_key,
    applicant_id,
    user_id,
    current_step,
    application_status,
    current_step_status,
    application_datetime,
    selected_for_work_node_name,
    selected_for_parent_work_node
from {{ ref('int_pc_fellow_applicant_data') }}
