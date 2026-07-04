{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partners') }}
)
select
    id::integer as partner_id,
    partner_name,
    city_id::integer as city_id,
    state_id::integer as state_id,
    pincode::integer as pincode,
    removed::boolean as is_removed,
    interested::boolean as interested,
    lead_source,
    school_type,
    partner_affiliation_type,
    address_line_1,
    address_line_2,
    total_child_count::integer as total_child_count,
    low_income_resource::boolean as low_income_resource,
    classes,
    created_by::integer as created_by,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
