{{ config(materialized='table') }}

with raw_master_mapping_sheet as (
    select * from {{ source('opschapter_google_sheet_raw', 'master_mapping_sheet') }}
)
select
    nullif(trim("City_Name"), '')               as city_name,
    nullif(trim("State"), '')                   as state,
    nullif(trim("CO_ID"), '')::integer          as co_id,
    nullif(trim("CO_Name"), '')                 as sheet_co_name,
    nullif(trim("Chapter_ID"), '')              as chapter_id,
    nullif(trim("Chapter_Name"), '')            as chapter_name,
    nullif(trim("Worknode_ID"), '')::integer    as worknode_id,
    nullif(trim("CHO_ID"), '')::integer         as cho_id,
    nullif(trim("CHO_Name"), '')                as sheet_cho_name,
    nullif(trim("Engine"), '')                  as engine,
    nullif(trim("Chapter_Status"), '')          as chapter_status,
    nullif(trim("CHO_Status"), '')              as cho_status,
    nullif(trim("Sourcing_Campaign_Code"), '')  as sourcing_campaign_code,
    nullif(trim("Campaign_Name"), '')           as campaign_name,
    nullif(trim("Fundraiser_ID"), '')           as fundraiser_id,
    nullif(trim("Fundraiser_Name"), '')         as fundraiser_name,
    nullif(trim("CHO_Allocation_on_PC_"), '')   as cho_allocation_on_pc,
    nullif(trim("_Validation_Status"), '')      as sheet_validation_status,
    nullif(trim("_Validation_Issues"), '')      as sheet_validation_issues,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from raw_master_mapping_sheet
