{{ config(materialized='table') }}

-- Volunteer Allocation History: volunteer-slot allocations with rich context
-- Built directly from bubble/CRM intermediate models (NOT dim_slot/dim_child/dim_crm_partner or
-- int_bubble__volunteer_class_child_detail) to match the legacy dbt model's row-inclusion rules
-- one-to-one, without changing those shared dims/intermediate for every other consumer:
--   - keeps allocations tied to removed slots/children (legacy didn't filter those out)
--   - excludes allocations tied to removed partners (legacy did filter that out)
--   - requires a matching Platform Commons user record (legacy used an inner join)

SELECT
    scsv.volunteer_sk,
    scs.slot_sk,
    scs.class_section_sk,
    scsv.volunteer_id,
    scsv.slot_class_section_id,
    scsv.created_date AS start_date,
    CASE WHEN scsv.is_removed THEN scsv.modified_date END AS end_date,
    NOT scsv.is_removed AS is_active,
    scs.slot_id,
    scs.class_section_subject_id,
    cs.section_name,
    p.crm_partner_id AS partner_id,
    p.partner_name,
    ud."UserId" AS user_id,
    ud."UserDisplayName" AS user_display_name,
    ud."Contact" AS contact,
    ud."Email" AS email,
    ud."UserLogin" AS user_login,
    sl.day_of_week,
    sl.slot_name,
    ch.child_id,
    ch.first_name AS child_first_name,
    ch.last_name AS child_last_name,
    c.class_name,
    sub.subject_name
FROM {{ ref('int_bubble__slot_class_section_volunteer') }} scsv
JOIN {{ ref('int_bubble__slot_class_section') }} scs
    ON scsv.slot_class_section_id = scs.slot_class_section_id
    AND scs.is_removed = FALSE
JOIN {{ ref('int_bubble__class_section') }} cs
    ON scs.class_section_id = cs.class_section_id
    AND cs.is_removed = FALSE
    AND cs.is_active = TRUE
JOIN {{ ref('int_pc_user_data') }} ud
    ON scsv.volunteer_id::numeric = ud."UserId"::numeric
JOIN {{ ref('int_bubble__slot') }} sl
    ON scs.slot_id = sl.slot_id
JOIN {{ ref('int_bubble__school_class') }} sc
    ON cs.school_class_id = sc.school_class_id
JOIN {{ ref('stg_bubble__class') }} c
    ON sc.class_id = c.class_id
JOIN {{ ref('int_bubble__class_section_subject') }} css
    ON scs.class_section_subject_id = css.class_section_subject_id
JOIN {{ ref('dim_subject') }} sub
    ON css.subject_id = sub.subject_id
JOIN {{ ref('int_bubble__child_class_section') }} ccs
    ON cs.class_section_id = ccs.class_section_id
    AND ccs.is_removed = FALSE
JOIN {{ ref('int_bubble__children') }} ch
    ON ccs.child_id = ch.child_id
JOIN {{ ref('dim_crm_partner') }} p
    ON cs.school_id = p.crm_partner_id
    AND p.is_removed = FALSE
ORDER BY scsv.volunteer_id, start_date
