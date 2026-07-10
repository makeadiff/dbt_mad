{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'gender') }}
)

select
    "dataCode"::text as gender_data_code,
    label::text as gender_label,
    "languageLanguageCode"::text as language_code,
    row_number() over (partition by "dataCode" order by "languageLanguageCode" = 'ENG' desc)::text as rn
from raw
