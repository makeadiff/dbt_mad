{{ config(materialized='table') }}

/*
TABLE SOURCE MAPPING FOR volunteer_class_child_view:

1. prod.slot_class_section_volunteer → FROM intermediate bubble/slot_class_section_volunteer_int → FROM bubble_staging.slot_class_section_volunteer
2. prod.slot_class_section → FROM intermediate bubble/slot_class_section_int → FROM bubble_staging.slot_class_section  
3. prod.class_section → FROM intermediate bubble/class_section_int → FROM bubble_staging.class_section
4. prod.user_data → FROM intermediate platform_commons/user_data_int → FROM source_platform_commons.user_data_int
5. prod.slot → FROM intermediate bubble/slot_int → FROM bubble_staging.slot
6. prod.school_class → FROM intermediate bubble/school_class_int → FROM bubble_staging.school_class
7. prod.class → FROM intermediate bubble/class_int → FROM bubble_staging.class
8. prod.class_section_subject → FROM intermediate bubble/class_section_subject_int → FROM bubble_staging.class_section_subject
9. prod.subject → FROM intermediate bubble/subject_int → FROM bubble_staging.subject
10. prod.child_class_section → FROM intermediate bubble/child_class_section_int → FROM bubble_staging.child_class_section
11. prod.child → FROM intermediate bubble/child_int → FROM bubble_staging.child
12. prod.partners → FROM intermediate crm_data/partners_int → FROM crm_data.partners

SCHEMA SOURCES:
- bubble_staging: Contains operational data from Bubble platform (volunteers, classes, children, etc.)
- source_platform_commons: Contains user management data 
- crm_data: Contains partner/organization data

DATA FLOW: Raw Sources → Intermediate Models (staging/cleaning) → Prod Models (final business logic)
*/

SELECT 
    scsv.volunteer_id,
    scs.slot_id,
    scs.class_section_subject_id,
    cs.section_name,
    p.id AS partner_id,
    p.partner_name,
    ud.user_id,
    ud.user_display_name,
    ud.contact,
    ud.email,
    ud.user_login, 
    s.day_of_week,
    s.slot_name,
    ch.child_id,
    ch.first_name AS child_first_name,
    ch.last_name AS child_last_name,
    c.class_name,
    sub.subject_name,
    scs.slot_class_section_id
FROM {{ ref('slot_class_section_volunteer_int') }} scsv
JOIN {{ ref('slot_class_section_int') }} scs
  ON scsv.slot_class_section_id = scs.slot_class_section_id
JOIN {{ ref('class_section_int') }} cs
  ON scs.class_section_id = cs.class_section_id
JOIN {{ ref('user_data_int') }} ud
  ON scsv.volunteer_id::text = ud.user_id
JOIN {{ ref('slot_int') }} s
  ON scs.slot_id = s.slot_id
JOIN {{ ref('school_class_int') }} sc
  ON cs.school_class_id = sc.school_class_id
JOIN {{ ref('class_int') }} c
  ON sc.class_id = c.class_id
JOIN {{ ref('class_section_subject_int') }} css
  ON scs.class_section_subject_id = css.class_section_subject_id
  AND css.removed = FALSE
JOIN {{ ref('subject_int') }} sub
  ON css.subject_id = sub.subject_id
  AND sub.removed = FALSE
JOIN {{ ref('child_class_section_int') }} ccs
  ON cs.class_section_id = ccs.class_section_id
  AND ccs.removed_boolean = FALSE
JOIN {{ ref('child_int') }} ch
  ON ccs.child_id = ch.child_id
JOIN {{ ref('partners_int') }} p
  ON cs.school_id::text = p.id
WHERE 
    scsv.removed = FALSE
    AND scs.removed = FALSE
    AND scs.is_active = TRUE
    AND cs.removed = FALSE
    AND cs.is_active = TRUE

