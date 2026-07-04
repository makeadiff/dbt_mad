{{ config(materialized='view') }}

with source as (
    select * from {{ source('crm_raw', 'partner_agreements') }}
)
select
    id::integer as partner_agreement_id,
    partner_id::integer as partner_id,
    current_status,
    conversion_stage,
    specific_doc_name,
    specific_doc_required::boolean as specific_doc_required,
    agreement_drop_date::date as agreement_drop_date,
    non_conversion_reason,
    potential_child_count::integer as potential_child_count,
    expected_conversion_day::integer as expected_conversion_day,
    removed::boolean as is_removed,
    "createdAt"::timestamp as created_at,
    "updatedAt"::timestamp as updated_at,
    _airbyte_raw_id,
    _airbyte_extracted_at::timestamp as _airbyte_extracted_at,
    _airbyte_meta
from source
