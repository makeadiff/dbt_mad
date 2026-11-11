{{ config(materialized='table') }}

-- Unpacks the three DOTS assessment forms (numeracy, literacy, life skills) into one row per
-- submission, following the int_dots__child_attendance pattern.
-- Flow: stg_dots__baseline_{numeracy,literacy,life_skills} → int_dots__baseline_submissions
-- Grain: one row per raw document (record_id is the raw _id; unique within a form).
--
-- Where the IDs come from: unlike attendance, these forms have no childId / volunteerId tags.
-- The child sits in a child-picker field inside main: under a random form key for numeracy (ouj84t)
-- and literacy (mdyfzc), and under the named key <prefix>childName for life skills (Dots, 10 Nov 2026).
-- Shape: {value: <childId>, tagId: "<slotClassSectionId>_<names>_<volunteerId>"}.
-- The picker is found by shape (any object in main carrying tagId) or by a key ending in _childName,
-- so a form edit that changes the random key does not break this model.
--
-- Every round and every status is kept (baseline, midline 1-3, endline; draft, published,
-- editPublished). Nothing is filtered here: downstream models decide what counts.
-- Rows with no main are drafts opened and never filled.
--
-- Life skills: until its Dalgo stream is resynced, main is null, so its rows carry no child, circle,
-- volunteer, round or grade (only who submitted, when, and the status). They fill in on the first
-- run after the resync with no change here.
--
-- Personal data: main.title and the picker's display / tagId hold child and volunteer names and
-- are deliberately not carried.

with numeracy as (
    select
        'numeracy'::text                 as form,
        'baselineAssessments_'::text     as field_prefix,
        "_id", main, meta, last_activity,
        kp_date_created, kp_date_last_saved, kp_published_status, _airbyte_extracted_at
    from {{ ref('stg_dots__baseline_numeracy') }}
),

literacy as (
    select
        'literacy'::text                                      as form,
        'baselineAssessmentsFoundationalLiteracyP_'::text     as field_prefix,
        "_id", main, meta, last_activity,
        kp_date_created, kp_date_last_saved, kp_published_status, _airbyte_extracted_at
    from {{ ref('stg_dots__baseline_literacy') }}
),

life_skills as (
    select
        'life_skills'::text                                   as form,
        'baselineAssessmentsFoundationalLifeSkill_'::text     as field_prefix,
        "_id", main, meta, last_activity,
        kp_date_created, kp_date_last_saved, kp_published_status, _airbyte_extracted_at
    from {{ ref('stg_dots__baseline_life_skills') }}
),

unioned as (
    select * from numeracy
    union all
    select * from literacy
    union all
    select * from life_skills
),

with_picker as (
    select
        u.*,
        p.picker_key,
        p.picker
    from unioned u
    left join lateral (
        select e.key as picker_key, e.value as picker
        from jsonb_each(coalesce(u.main, '{}'::jsonb)) e
        where jsonb_typeof(e.value) = 'object'
          and (e.value ? 'tagId' or e.key like '%\_childName')
        order by (e.key like '%\_childName') desc, e.key
        limit 1
    ) p on true
),

unpacked as (
    select
        *,
        nullif(trim(picker ->> 'value'), '')                                        as picker_value,
        nullif(trim(picker ->> 'tagId'), '')                                        as picker_tag_id,
        nullif(trim(main #>> array[field_prefix || 'assessmentNumber', 'value']), '')            as round_raw,
        nullif(trim(main #>> array[field_prefix || 'selectTheGradeTheStudentIsIn', 'value']), '') as grade_raw,
        nullif(trim(main ->> (field_prefix || 'selectTheGradeTheStudentIsIn_v1')), '')            as grade_v1_raw,
        nullif(trim(main #>> array[field_prefix || 'finalLearningGradeLevel', 'value']), '')      as level_raw,
        nullif(trim(main ->> (field_prefix || 'finalLearningGradeLevel_v1')), '')                 as level_v1_raw
    from with_picker
)

select
    -- Identifiers
    "_id"                                                                           as record_id,
    form,
    case when picker_value ~ '^\d+$' then picker_value::integer end                 as child_id,
    case when picker_tag_id ~ '^\d+_' then split_part(picker_tag_id, '_', 1)::integer end
                                                                                    as slot_class_section_id,
    (regexp_match(picker_tag_id, '_(\d+)$'))[1]::integer                            as volunteer_id,

    -- Assessment info. Grade and level come either as a picked option ({value: "grade5"}) or as a
    -- plain number in the _v1 field (the newer form version), so both are read.
    round_raw                                                                       as assessment_round,
    coalesce(
        nullif(regexp_replace(grade_raw, '\D', '', 'g'), '')::integer,
        case when grade_v1_raw ~ '^\d+(\.\d+)?$' then grade_v1_raw::numeric::integer end
    )                                                                               as child_grade,
    coalesce(
        nullif(regexp_replace(level_raw, '\D', '', 'g'), '')::integer,
        case when level_v1_raw ~ '^\d+(\.\d+)?$' then level_v1_raw::numeric::integer end
    )                                                                               as final_learning_grade_level,

    -- Status and dates
    nullif(trim(kp_published_status), '')                                           as published_status,
    coalesce(kp_published_status, '') in ('published', 'editPublished')             as is_published,
    main is not null                                                                as has_form_data,
    nullif(trim(kp_date_created), '')::timestamp                                    as created_date,
    nullif(trim(kp_date_last_saved), '')::timestamp                                 as last_saved_date,
    nullif(trim(last_activity ->> 'date'), '')::timestamp                           as last_activity_date,

    -- Form metadata
    nullif(trim(meta ->> 'kp_contributed_by'), '')                                  as author,
    picker_key                                                                      as child_picker_key,
    _airbyte_extracted_at

from unpacked
