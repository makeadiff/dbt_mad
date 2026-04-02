{{ config(materialized='table') }}

-- Active partners: CRM partners with latest converted agreement + latest CO assignment
-- Eliminates repetitive CTE patterns across multiple analytics models
-- Used by: prod_class_ops_data, prod_volunteer_recruitment, prod_school_data_comparison, etc.

with latest_partner_cos as (
    select
        partner_id,
        co_id as co_user_id
    from (
        select
            partner_id,
            co_id,
            row_number() over (
                partition by partner_id 
                order by updated_at desc, created_at desc, partner_co_id desc
            ) as rn
        from {{ ref('int_crm__partner_cos') }}
    ) ranked
    where rn = 1
),

latest_agreements as (
    select
        partner_id,
        conversion_stage,
        created_at
    from (
        select
            partner_id,
            conversion_stage,
            created_at,
            row_number() over (
                partition by partner_id 
                order by created_at desc, agreement_id desc
            ) as rn
        from {{ ref('fct_partner_agreements') }}
    ) ranked
    where rn = 1
)

SELECT 
    p.crm_partner_id AS partner_id,
    p.partner_name,
    p.partner_affiliation_type,
    pco.co_user_id,
    v.volunteer_id AS co_id_numeric,
    v.volunteer_name AS co_name,
    la.conversion_stage,
    la.created_at AS agreement_created_at
FROM {{ ref('dim_crm_partner') }} p
LEFT JOIN latest_partner_cos pco
    ON p.crm_partner_id = pco.partner_id
JOIN latest_agreements la
    ON p.crm_partner_id = la.partner_id
    AND la.conversion_stage = 'converted'
LEFT JOIN {{ ref('dim_volunteer') }} v
    ON pco.co_user_id = v.volunteer_id
WHERE p.is_removed = FALSE
