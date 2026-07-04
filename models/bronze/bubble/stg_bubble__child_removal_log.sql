{{ config(materialized='view') }}

with source as (
    select * from {{ source('bubble_raw', 'child_removal_log') }}
)
select
    "child_removal_log_id_number"::integer as child_removal_log_id,
    "child_id_custom_child" as child_id,
    "co_id_user" as co_id,
    "other_details_text" as other_details,
    "removal_reason_option_student_delete_reason" as removal_reason,
    "removed_boolean"::boolean as is_removed,
    "school_id_custom_partner" as school_id,
    "Created_Date"::date as created_date,
    "Modified_Date"::date as modified_date,
    "_airbyte_raw_id",
    "_airbyte_extracted_at"::timestamp as _airbyte_extracted_at,
    "_airbyte_meta"
from source
