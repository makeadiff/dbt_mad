{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'scLevelBatch_batchName_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "scLevelBatchId"::bigint as sc_level_batch_id,
    "batchNameId"::bigint as batch_name_id
from raw
