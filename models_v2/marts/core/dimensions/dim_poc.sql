{{ config(materialized='table') }}

-- dim_poc: One row per Point of Contact
-- Flow: int_crm__pocs → dim_poc

select
    poc_id,
    partner_id,
    poc_name,
    poc_email,
    poc_contact,
    poc_designation,
    date_of_first_contact,
    is_removed,
    created_at,
    updated_at
from {{ ref('int_crm__pocs') }}
