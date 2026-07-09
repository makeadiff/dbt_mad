{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'user') }}
)

select
    id::bigint as user_id,
    "personId"::bigint as person_id,
    
    login,
    "firstName"::text as first_name,
    "middleName"::text as middle_name,
    "lastName"::text as last_name,
    
    "isActive"::boolean as is_active,
    
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    "xModifiedTimestamp"::timestamp as modified_datetime,

    "xIsDeleted"::boolean as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
