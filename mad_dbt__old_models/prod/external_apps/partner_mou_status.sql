{{ config(materialized='table') }}

WITH agreement_ranking AS (
  SELECT
    partner_id,
    conversion_stage,
    current_status,
    non_conversion_reason,
    created_at,
    ROW_NUMBER() OVER (
      PARTITION BY partner_id
      ORDER BY created_at DESC NULLS LAST, id DESC
    ) AS rn
  FROM {{ ref('partner_agreements_int') }}
),

latest_mous AS (
  SELECT
    partner_id,
    created_at
  FROM (
    SELECT
      partner_id,
      created_at,
      ROW_NUMBER() OVER (
        PARTITION BY partner_id
        ORDER BY created_at DESC NULLS LAST, id DESC
      ) AS rn
    FROM {{ ref('mous_int') }}
  ) m_sub
  WHERE rn = 1
),

partner_status_logic AS (
  SELECT
    r1.partner_id,
    r1.non_conversion_reason,
    r1.created_at AS latest_agreement_date,
    CASE
      WHEN r1.conversion_stage = 'converted'
        AND r1.current_status = 'renewed'
        THEN 'renewed'

      WHEN r1.conversion_stage = 'dropped'
        AND r1.current_status = 'closed_not_renewed'
        THEN 'not_renewed'

      WHEN r1.conversion_stage = 'converted'
        AND (r1.current_status = '' OR r1.current_status IS NULL)
        THEN 'active partners'

      WHEN r1.conversion_stage = 'dropped'
        AND r2.conversion_stage = 'converted'
        AND (r1.current_status = '' OR r1.current_status IS NULL)
        THEN 'older active partners'

      ELSE 'other'
    END AS partner_category
  FROM agreement_ranking r1
  LEFT JOIN agreement_ranking r2
    ON r1.partner_id = r2.partner_id
    AND r2.rn = 2
  WHERE r1.rn = 1
)

SELECT
  p.id AS partner_id,
  p.partner_name,
  psl.partner_category,
  psl.non_conversion_reason,
  psl.latest_agreement_date,
  lm.created_at AS latest_mou_date,
  CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata' AS dbt_run_at
FROM {{ ref('partners_int') }} p
INNER JOIN partner_status_logic psl
  ON p.id = psl.partner_id
LEFT JOIN latest_mous lm
  ON p.id = lm.partner_id
WHERE psl.partner_category != 'other'
