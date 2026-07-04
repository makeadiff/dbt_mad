{{ config(materialized='view') }}

select
    "scLevelBatchId"::bigint as sc_level_batch_id,
    "scLevelBatchEventsId"::bigint as batch_event_id
from {{ source('pc_raw', 'scLevelBatch_scLevelBatchEvents_bridge') }}
