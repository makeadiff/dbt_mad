{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'city') }}
)

select
    "dataCode"::text as city_data_code,
    label::text as city_name,
    "languageLanguageCode"::text as language_code,
    row_number() over (partition by "dataCode" order by "languageLanguageCode" = 'ENG' desc)::text as rn
from raw
