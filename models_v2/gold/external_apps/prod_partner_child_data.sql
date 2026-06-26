{{ config(materialized='table') }}

WITH partner_data AS (
  SELECT
    p.bubble_partner_id AS partner_id,
    p.city,
    p.partner_name,
    p.partner_co_id_user AS co_id_user,
    u.user_id_number AS co_id,
    u.user_display_name AS co_name
  FROM {{ ref('dim_bubble_partner') }} p
  LEFT JOIN {{ ref('stg_bubble__user') }} u
    ON p.partner_co_id_user = u.user_id
  WHERE p.is_removed = false
),

child_data AS (
  SELECT
    child_id,
    first_name as child_first_name,
    last_name as child_last_name,
    gender,
    school_id,
    is_active
  FROM {{ ref('dim_child') }}
  WHERE is_active = true
    AND is_removed = false
),

-- Get latest child_class record for each child (based on created_date)
child_class_latest AS (
  SELECT
    cc.child_id as child_uuid,
    c.child_id as child_integer_id,
    cc.school_class_id as school_class_uuid,
    sc.school_class_id as school_class_integer_id,
    ROW_NUMBER() OVER (PARTITION BY cc.child_id ORDER BY cc.created_date DESC) as rn
  FROM {{ ref('stg_bubble__child_class') }} cc
  LEFT JOIN {{ ref('stg_bubble__children') }} c
    ON cc.child_id = c._id
  LEFT JOIN {{ ref('stg_bubble__school_class') }} sc
    ON cc.school_class_id = sc._id
  WHERE cc.is_removed = false
),

school_class_data AS (
  SELECT
    school_class_id,
    class_id as class_uuid
  FROM {{ ref('stg_bubble__school_class') }}
  WHERE is_removed = false
),

class_data AS (
  SELECT
    _id as class_uuid,
    class_name
  FROM {{ ref('stg_bubble__class') }}
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
  ON cd.child_id = ccl.child_integer_id
  AND ccl.rn = 1
LEFT JOIN school_class_data scd
  ON ccl.school_class_integer_id = scd.school_class_id
LEFT JOIN class_data cls
  ON scd.class_uuid = cls.class_uuid
