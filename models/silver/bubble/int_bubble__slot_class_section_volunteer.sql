{{ config(materialized='table') }}

-- Deduplicates slot_class_section_volunteer records
-- Flow: stg_bubble__slot_class_section_volunteer → int_bubble__slot_class_section_volunteer
-- slot_class_section_id and volunteer_id come pre-resolved from sessionops_raw (real bigint
-- FKs) - no more UUID joins needed here now that bronze sources from sessionops_raw.
-- deleted_at (2026-09-12) is carried through from staging -- it's the real removal timestamp,
-- 100% populated on every is_removed=true row (confirmed against 1220 removed rows warehouse-wide),
-- unlike modified_date which can move for reasons unrelated to removal. Needed by
-- fct_e2_planned_session_status to reconstruct, for a past date, whether a volunteer assignment was
-- actually active on that date (created_date <= date <= deleted_at), not just whether one exists today.

with joined as (
    select
        raw.slot_class_section_volunteer_id,
        raw.slot_class_section_id,
        raw.volunteer_id,
        raw.is_active,
        raw.is_removed,
        raw.created_date,
        raw.modified_date,
        raw.deleted_at
    from {{ ref('stg_bubble__slot_class_section_volunteer') }} raw
),

deduplicated as (
    {{ dbt_utils.deduplicate(
        relation='joined',
        partition_by='slot_class_section_volunteer_id',
        order_by='modified_date desc',
       )
    }}
)

select
    {{ dbt_utils.generate_surrogate_key(['slot_class_section_volunteer_id']) }} as volunteer_assignment_sk,
    {{ dbt_utils.generate_surrogate_key(['slot_class_section_id']) }} as slot_class_section_sk,
    {{ dbt_utils.generate_surrogate_key(['volunteer_id']) }} as volunteer_sk,
    slot_class_section_volunteer_id,
    slot_class_section_id,
    volunteer_id,
    is_active,
    is_removed,
    created_date,
    modified_date,
    deleted_at
from deduplicated
