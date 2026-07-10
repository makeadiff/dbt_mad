{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'userCreditPoint_creditHistories_bridge') }}
)

select
    "generated_id"::text as generated_id,
    "userCreditPointId"::bigint as user_credit_point_id,
    "creditHistoriesId"::bigint as credit_histories_id
from raw
