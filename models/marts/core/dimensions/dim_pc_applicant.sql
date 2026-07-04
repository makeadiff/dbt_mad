{{ config(materialized='table') }}

with applicants as (
    select distinct
        applicant_key,
        user_key,
        "ApplicationID" as applicant_id,
        "UserId" as user_id,
        "FirstName" as first_name,
        "LastName" as last_name,
        "DisplayName" as display_name,
        "PrimaryEmailAddress" as primary_email,
        "MobileNumber" as mobile_number,
        "DateOfBirth" as date_of_birth,
        "Gender" as gender,
        "City" as city,
        "State" as state,
        "Country" as country,
        "Pincode" as pincode
    from {{ ref('int_pc_applicant_data') }}
)

select * from applicants
