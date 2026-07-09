{{ config(materialized='table') }}

-- User School Chapter Mapping: CO-to-converted-partner mapping
-- active_partners/agreement/CO logic is built inline (not from int_crm__active_partners, which is
-- shared with other reports) to preserve old user_school_chapter_mapping.sql semantics: latest
-- agreement/CO tie-broken by updated_at only, and CO name/email resolved without a MAD-role
-- requirement.

WITH active_partners AS (
    SELECT
        crm_partner_id AS school_id,
        partner_name AS school_name
    FROM {{ ref('dim_crm_partner') }}
    WHERE is_removed = false
),

latest_partner_agreements AS (
    SELECT partner_id, conversion_stage, updated_at
    FROM (
        SELECT
            partner_id,
            conversion_stage,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY updated_at DESC) AS rn
        FROM {{ ref('int_crm__partner_agreements') }}
        WHERE is_removed = false
    ) ranked
    WHERE rn = 1
),

latest_partner_cos AS (
    SELECT partner_id, co_id
    FROM (
        SELECT
            partner_id,
            co_id,
            ROW_NUMBER() OVER (PARTITION BY partner_id ORDER BY updated_at DESC) AS rn
        FROM {{ ref('int_crm__partner_cos') }}
    ) ranked
    WHERE rn = 1
),

partners_with_cos AS (
    SELECT
        ap.school_id,
        ap.school_name,
        pco.co_id
    FROM active_partners ap
    LEFT JOIN latest_partner_cos pco
        ON ap.school_id = pco.partner_id
),

converted_partners_with_cos AS (
    SELECT
        pwc.school_id,
        pwc.school_name,
        pwc.co_id
    FROM partners_with_cos pwc
    INNER JOIN latest_partner_agreements lpa
        ON pwc.school_id = lpa.partner_id
    WHERE lpa.conversion_stage = 'converted'
),

-- CO display name/email resolved directly (no MAD-role requirement), matching old semantics —
-- int_pc_user_data would drop COs without a qualifying non-applicant role.
co_lookup AS (
    SELECT
        u.user_id,
        u.first_name || ' ' || COALESCE(u.last_name, '') AS user_name,
        con.email AS user_email
    FROM {{ ref('stg_pc_user') }} u
    LEFT JOIN (
        SELECT
            pcb.person_id,
            MAX(CASE WHEN c.contact_type IN ('CONTACT_TYPE.MAIL', 'MAIL') THEN c.contact_value END) AS email
        FROM {{ ref('stg_pc_person_person_contacts_bridge') }} pcb
        JOIN {{ ref('stg_pc_person_contacts') }} pc ON pcb.person_contact_id = pc.person_contact_id
        JOIN {{ ref('stg_pc_contact') }} c ON pc.contact_id = c.contact_id
        GROUP BY pcb.person_id
    ) con ON u.person_id = con.person_id
)

SELECT
    cpwc.school_id,
    cpwc.school_name,
    cpwc.co_id AS user_id,
    col.user_name,
    col.user_email
FROM converted_partners_with_cos cpwc
LEFT JOIN co_lookup col
    ON cpwc.co_id = col.user_id
ORDER BY cpwc.school_name, col.user_name
