{{ config(materialized='table') }}

-- Chapter Mapping: validated Chapter/CO/CHO/Fundraiser/Campaign roster (E1/E2 chapters)
-- Source: int_google_sheet__chapter_mapping (ported from legacy chapter_mapping_int)

select
    city_name,
    state,
    co_id,
    co_name,
    chapter_id,
    chapter_name,
    worknode_id,
    cho_id,
    cho_name,
    engine,
    chapter_status,
    sourcing_campaign_code,
    campaign_name,
    fundraiser_id,
    chapter_validation,
    fundraiser_name
from {{ ref('int_google_sheet__chapter_mapping') }}
where chapter_validation = true
