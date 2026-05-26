{{ config(materialized='table') }}

with raw_child_removal_log as (
    select * from {{ source('bubble_staging', 'child_removal_log') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_co as (
    select "_id", user_id from {{ source('bubble_staging', 'user') }}
)
select
    raw.child_removal_log_id::integer       as child_removal_log_id,
    ref_ch.child_id::integer                as child_id,
    ref_co.user_id::integer                 as co_id,
    ref_p.partner_id::integer               as school_id,
    raw.other_details,
    raw.removal_reason,
    raw.removed,
    raw.is_active,
    raw.academic_year,
    raw."Created_Date"                      as created_date,
    raw."Modified_Date"                     as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_removal_log raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_co ref_co
    on ref_co."_id" = raw.co_id
