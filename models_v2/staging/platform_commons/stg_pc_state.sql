{{ config(materialized='view') }}

with raw as (
    select * from {{ source('pc_raw', 'state') }}
)

select
    "dataCode"::text as state_data_code,
    label::text as state_name,
    "languageLanguageCode"::text as language_code,
    row_number() over (partition by "dataCode" order by "languageLanguageCode" = 'ENG' desc)::text as rn
from raw
