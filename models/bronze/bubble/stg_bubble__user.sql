{{ config(materialized='view') }}

with raw_user as (
    select * from {{ source('bubble_raw', 'user') }}
)
select
    "_id" as user_id,
    "city" as city,
    "state" as state,
    "center" as center,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "authentication",
    "contact"::numeric as contact_number,
    "user_id"::integer as user_id_number,
    "user_role" as user_role,
    "user_signed_up",
    "user_login" as user_login,
    "temp_password" as updated_password,
    "user_display_name" as user_display_name,
    "reporting_manager_role_code" as reporting_manager_role_code,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_user
