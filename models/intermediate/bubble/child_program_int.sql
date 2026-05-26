{{ config(materialized='table') }}

with raw_child_program as (
    select * from {{ source('bubble_staging', 'child_program') }}
),
ref_child as (
    select "_id", child_id from {{ source('bubble_staging', 'child') }}
),
ref_program as (
    select "_id", program_id from {{ source('bubble_staging', 'program') }}
)
select
    raw.child_program_id::integer       as child_program_id,
    raw.academic_year,
    ref_ch.child_id::integer            as child_id,
    ref_prog.program_id::integer        as program_id,
    raw.removed                         as removed_boolean,
    raw."Created_Date"                  as created_date,
    raw."Modified_Date"                 as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_child_program raw
left join ref_child ref_ch
    on ref_ch."_id" = raw.child_id
left join ref_program ref_prog
    on ref_prog."_id" = raw.program_id
