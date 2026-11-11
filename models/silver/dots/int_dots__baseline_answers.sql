{{ config(materialized='table') }}

-- Every field value from the DOTS numeracy, literacy and life skills assessment forms, one row per submission
-- per field. Keeps all answers without hand-listing 40+ columns per form, so a question added to
-- the form shows up here on the next run with no model change.
-- Flow: stg_dots__baseline_{numeracy,literacy,life_skills} → int_dots__baseline_answers
-- Grain: one row per (form, record_id, field_key). Join to int_dots__baseline_submissions on
-- (form, record_id) for the child, circle, volunteer, round and status.
--
-- field_key is the form's own field name with the form prefix removed (for example
-- a1Multiply4217). Fields the form builder left with a random key (for example dbj2we) are kept
-- under that key with is_named_field = false; what they hold is not confirmed yet.
-- A picked option lands as {value, display, reactKey}: answer_value is its value and
-- answer_display its label. Text and number answers land in answer_value as text.
--
-- Not carried (personal data): main.title and the child picker, which hold names. The child comes
-- from int_dots__baseline_submissions as an ID.
-- Life skills rows appear once its Dalgo stream is resynced with main (Dots, 10 Nov 2026).

with forms as (
    select
        'numeracy'::text as form, 'baselineAssessments_'::text as field_prefix, "_id", main
    from {{ ref('stg_dots__baseline_numeracy') }}
    where main is not null

    union all

    select
        'literacy'::text, 'baselineAssessmentsFoundationalLiteracyP_'::text, "_id", main
    from {{ ref('stg_dots__baseline_literacy') }}
    where main is not null

    union all

    select
        'life_skills'::text, 'baselineAssessmentsFoundationalLifeSkill_'::text, "_id", main
    from {{ ref('stg_dots__baseline_life_skills') }}
    where main is not null
),

fields as (
    select
        f.form,
        f."_id",
        f.field_prefix,
        e.key   as raw_key,
        e.value as raw_value
    from forms f
    cross join lateral jsonb_each(f.main) e
    where e.key <> 'title'
      and not (jsonb_typeof(e.value) = 'object' and e.value ? 'tagId')
      and e.key not like '%\_childName'
      and jsonb_typeof(e.value) <> 'null'
)

select
    form,
    "_id"                                                              as record_id,
    case
        when raw_key like field_prefix || '%' then substr(raw_key, length(field_prefix) + 1)
        else raw_key
    end                                                                as field_key,
    raw_key like field_prefix || '%'                                   as is_named_field,
    jsonb_typeof(raw_value)                                            as value_type,
    nullif(trim(
        case
            when jsonb_typeof(raw_value) = 'object' then raw_value ->> 'value'
            else raw_value #>> '{}'
        end
    ), '')                                                             as answer_value,
    case
        when jsonb_typeof(raw_value) = 'object' then nullif(trim(raw_value ->> 'display'), '')
    end                                                                as answer_display
from fields
