{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'personProfile') }}
)

select
    id as person_profile_id,
    
    "firstName" as first_name,
    "lastName" as last_name,
    
    "dateOfBirth" as date_of_birth,
    "genderIdentifier" as gender_identifier,
    "genderDataCode" as gender_data_code,
    
    "currentProfessionalStatus" as current_professional_status,
    "currentlyStudying" as currently_studying,
    "currentlyWorking" as currently_working,
    occupation,
    "areaOfSpecialization" as area_of_specialization,
    
    "createdDateTime" as created_datetime,
    "updatedDateTime" as updated_datetime,
    
    "xIsDeleted" as is_deleted

from raw
where "xIsDeleted" is false or "xIsDeleted" is null
