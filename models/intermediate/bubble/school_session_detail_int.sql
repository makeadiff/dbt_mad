{{ config(materialized='table') }}

with raw_school_session_detail as (
    select * from {{ source('bubble_staging', 'school_session_detail') }}
),
ref_partner as (
    select "_id", partner_id from {{ source('bubble_staging', 'partner') }}
),
ref_school_academic_year as (
    select "_id", school_academic_year_id from {{ source('bubble_staging', 'school_academic_year') }}
)
select
    raw.session_id::integer                              as session_id,
    ref_p.partner_id::integer                           as school_id,
    ref_say.school_academic_year_id::integer            as school_academic_year_id,
    raw.start_date::timestamp                           as start_date,
    raw.end_date::timestamp                             as end_date,
    raw.is_active::boolean                              as is_active,
    raw.removed::boolean                                as removed,
    raw."Created_By"                                    as created_by,
    raw."Created_Date"::timestamp                       as created_date,
    raw."Modified_Date"::timestamp                      as modified_date,
    raw."_airbyte_raw_id",
    raw."_airbyte_extracted_at",
    raw."_airbyte_meta"
from raw_school_session_detail raw
left join ref_partner ref_p
    on ref_p."_id" = raw.school_id
left join ref_school_academic_year ref_say
    on ref_say."_id" = raw.school_academic_year
