{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'user') }}
)

select
    id as user_id,
    "personId" as person_id,
    
    login,
    "firstName" as first_name,
    "middleName" as middle_name,
    "lastName" as last_name,
    
    "isActive" as is_active,
    "inActiveReason" as inactive_reason,
    
    "createdDateTime" as created_datetime,
    "updatedDateTime" as updated_datetime,
    
    "xIsDeleted" as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
