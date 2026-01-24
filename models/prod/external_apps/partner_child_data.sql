{{ config(materialized='table') }}

WITH partner_data AS (
  SELECT
    p.partner_id1 as partner_id,
    p.city,
    p.partner_name,
    p.co_id_user,
    u.user_id_number as co_id,
    u.user_display_name as co_name
  FROM {{ ref('partner_int') }} p
  LEFT JOIN {{ ref('user_int') }} u
    ON p.co_id_user = u.user_id
  WHERE p.removed = false
),

child_data AS (
  SELECT
    child_id,
    first_name as child_first_name,
    last_name as child_last_name,
    gender,
    school_id
  FROM {{ ref('child_int') }}
  WHERE is_active = true
    AND removed = false
),

-- Get latest child_class record for each child (based on created_date)
child_class_latest AS (
  SELECT
    child_id,
    school_class_id,
    ROW_NUMBER() OVER (PARTITION BY child_id ORDER BY created_date DESC) as rn
  FROM {{ ref('child_class_int') }}
  WHERE removed_boolean = false
),

school_class_data AS (
  SELECT
    school_class_id,
    class_id
  FROM {{ ref('school_class_int') }}
  WHERE removed = false
),

class_data AS (
  SELECT
    class_id,
    class_name
  FROM {{ ref('class_int') }}
)

SELECT
  pd.partner_id::integer,
  pd.city,
  pd.partner_name,
  pd.co_id::integer,
  pd.co_name,
  cd.child_id::integer,
  cd.child_first_name,
  cd.child_last_name,
  cd.gender,
  cls.class_name
FROM partner_data pd
INNER JOIN child_data cd
  ON pd.partner_id = cd.school_id
LEFT JOIN child_class_latest ccl
  ON cd.child_id = ccl.child_id
  AND ccl.rn = 1
LEFT JOIN school_class_data scd
  ON ccl.school_class_id = scd.school_class_id
LEFT JOIN class_data cls
  ON scd.class_id = cls.class_id
