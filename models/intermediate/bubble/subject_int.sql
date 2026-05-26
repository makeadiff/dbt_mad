{{ config(materialized='table') }}

with raw_subject as (
    select * from {{ source('bubble_staging', 'subject') }}
),
ref_program as (
    select "_id", program_id from {{ source('bubble_staging', 'program') }}
)
select
    raw.subject_id::integer         as subject_id,
    raw.subject_name,
    raw.removed,
    ref_prog.program_id::integer    as program_id,
    raw."Created_By"                as created_by,
    raw."Created_Date"              as created_date,
    raw."Modified_Date"             as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_subject raw
left join ref_program ref_prog
    on ref_prog."_id" = raw.program_id
