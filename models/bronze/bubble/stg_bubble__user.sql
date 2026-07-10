{{ config(materialized='table') }}

with raw_user as (
    select * from {{ source('bubble_raw', 'user') }}
)
select
    "_id" as user_id,
    "city" as city,
    "state" as state,
    "center" as center,
    "is_active"::boolean as is_active,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "authentication",
    "contact"::numeric as contact_number,
    "user_id"::integer as user_id_number,
    "user_role" as user_role,
    "worknode_id"::integer as worknode_id,
    "user_signed_up",
    "user_login" as user_login,
    "temp_password" as updated_password,
    "user_display_name" as user_display_name,
    "reporting_manager_user_id"::numeric as reporting_manager_user_id,
    "reporting_manager_role_code" as reporting_manager_role_code,
    "reporting_manager_user_login" as reporting_manager_user_login,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_user
