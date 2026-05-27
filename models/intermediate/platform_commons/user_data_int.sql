{{ config(
  materialized='table',
  tags=["user_data"]
) }}


with source as (
  select * from {{ source('source_platform_commons', 'user_data_int') }}
),

renamed as (
  select
    "City"::text as city,
    "Email"::text as email,
    "State"::text as state,
    "Center"::text as center,
    "UserId"::numeric::bigint::text as user_id,
    "AddedBy"::text as added_by,
    "Contact"::text as contact,
    "UserRole"::text as user_role,
    "UserLogin"::text as user_login,
    "UserDisplayName"::text as user_display_name,
    "ReportingManagerUserId"::int as reporting_manager_user_id,
    "ReportingManagerRoleCode"::text as reporting_manager_role_code,
    "ReportingManagerUserLogin"::text as reporting_manager_user_login,
    
    -- Casting only if ISO 8601 or standard format
    case 
      when "UserCreatedDateTime" ~ '^\d{4}-\d{2}-\d{2}' then "UserCreatedDateTime"::timestamp
      else null
    end as user_created_datetime,

    case 
      when "UserUpdatedDateTime" ~ '^\d{4}-\d{2}-\d{2}' then "UserUpdatedDateTime"::timestamp
      else null
    end as user_updated_datetime,


    "_airbyte_raw_id"::text as _airbyte_raw_id,
    "_airbyte_extracted_at"::timestamptz as _airbyte_extracted_at,
    "_airbyte_meta"
  from source
)

 
  {{ dbt_utils.deduplicate(
      relation='renamed',
      partition_by='user_id',
      order_by='"user_updated_datetime" desc',
     )
  }}

