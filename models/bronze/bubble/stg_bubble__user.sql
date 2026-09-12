{{ config(materialized='table') }}

with raw_user as (
    select * from {{ source('sessionops_raw', 'users') }}
)
select
    "user_id"::bigint as user_id,
    "city" as city,
    "state" as state,
    "center" as center,
    "email" as email,
    "is_active"::boolean as is_active,
    "user_created_datetime"::date as created_date,
    "user_updated_datetime"::date as modified_date,
    null::jsonb as authentication, -- auth now lives in a separate UserAuth table, not synced into the warehouse
    "contact"::numeric as contact_number,
    "user_id"::bigint as user_id_number,
    "user_role" as user_role,
    "worknode_id"::integer as worknode_id,
    null::boolean as user_signed_up, -- dropped in sessionops, no equivalent field
    "user_login" as user_login,
    null::text as updated_password, -- auth now lives in a separate UserAuth table, not synced into the warehouse
    "user_display_name" as user_display_name,
    "added_by" as added_by,
    "synced_at"::timestamp as synced_at,
    "last_login_at"::timestamp as last_login_at,
    "deleted_at"::timestamp as deleted_at,
    "deleted_by_id"::bigint as deleted_by_id,
    "reporting_manager_user_id"::bigint as reporting_manager_user_id,
    "reporting_manager_role_code" as reporting_manager_role_code,
    "reporting_manager_user_login" as reporting_manager_user_login,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta",
    "_airbyte_generation_id"
from raw_user
