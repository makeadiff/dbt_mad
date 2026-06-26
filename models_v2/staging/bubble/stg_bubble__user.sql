{{ config(materialized='view') }}

with raw_user as (
    select * from {{ source('bubble_raw', 'user') }}
)
select
    "_id" as user_id,
    "city_text" as city,
    "state_text" as state,
    "center_text" as center,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "authentication",
    "contact_number",
    "user_id_number"::integer as user_id_number,
    "user_role_text" as user_role,
    "user_signed_up",
    "user_login_text" as user_login,
    "updated_password_text" as updated_password,
    "user_display_name_text" as user_display_name,
    "reporting_manager_role_code_text" as reporting_manager_role_code,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_user
