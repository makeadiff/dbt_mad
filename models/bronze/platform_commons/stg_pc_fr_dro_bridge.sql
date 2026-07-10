{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'feedbackResponses_defaultResponseOptions_bridge') }}
)

select
    "feedbackResponsesId"::bigint as feedback_response_id,
    "defaultResponseOptionsId"::bigint as default_response_option_id
from raw
