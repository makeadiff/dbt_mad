{{ config(materialized='table') }}

select
    credit_key,
    user_key,
    "CreditPointHistory" as points_awarded,
    "CreditPointHistoryReason" as reason,
    "TaggedAtType" as event_type
from {{ ref('int_pc_credit_data') }}
