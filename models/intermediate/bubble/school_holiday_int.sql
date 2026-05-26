{{ config(materialized='table') }}

with raw_school_holiday as (
    select * from {{ source('bubble_staging', 'school_holiday') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
)
select
    raw.school_holiday_id::integer      as school_holiday_id,
    ref_p.partner_id::integer           as school_id,
    raw.start_date,
    raw.end_date,
    raw.holiday_reason,
    raw.holiday_description,
    raw.is_active,
    raw.removed,
    raw."Created_By"                    as created_by,
    raw."Created_Date"                  as created_date,
    raw."Modified_Date"                 as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_holiday raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
