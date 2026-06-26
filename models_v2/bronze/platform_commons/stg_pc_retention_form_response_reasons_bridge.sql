{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'retentionFormResponse_reasons_bridge') }}
)

select
    generated_id::bigint as bridge_id,
    "reasonsId"::bigint as reason_id,
    "retentionFormResponseId"::bigint as retention_form_response_id
from raw
