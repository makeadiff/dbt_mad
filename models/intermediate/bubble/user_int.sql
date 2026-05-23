{{ config(materialized='table') }}

with raw_user as (
    select * from {{ source('bubble_staging', 'user') }}
)
select
    raw."_id" as user_id,
    raw.user_id as user_id_number,
    raw.city,
    raw.state,
    raw.center,
    raw.user_role,
    raw.user_login,
    raw.user_display_name,
    raw.user_signed_up,
    raw.is_active,
    raw.authentication,
    raw.contact as contact_number,
    raw.temp_password as updated_password,
    raw.reporting_manager_role_code,
    raw.reporting_manager_user_id,
    raw.reporting_manager_user_login,
    raw."Created_Date" as created_date,
    raw."Modified_Date" as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_user raw
