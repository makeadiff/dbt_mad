{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'personProfile') }}
)

select
    id::bigint as person_profile_id,
    
    "firstName"::text as first_name,
    "lastName"::text as last_name,
    
    "dateOfBirth"::text as date_of_birth,
    "genderIdentifier"::text as gender_identifier,
    "genderDataCode"::text as gender_data_code,
    
    "currentProfessionalStatus"::text as current_professional_status,
    occupation,
    "areaOfSpecialization"::text as area_of_specialization,
    
    "createdDateTime"::timestamp as created_datetime,
    "updatedDateTime"::timestamp as updated_datetime,
    
    "xIsDeleted"::boolean as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
