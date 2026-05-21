{{ config(materialized='table') }}

with retention as (
    select * from {{ ref('stg_pc_opportunity_applicant_retention') }}
),

form as (
    select * from {{ ref('stg_pc_retention_form_response') }}
),

users as (
    select * from {{ ref('stg_pc_user') }}
),

roles as (
    select
        user_id,
        string_agg(distinct role_code, ',') as current_roles
    from {{ ref('stg_pc_user_role') }}
    group by 1
),

worknode as (
    select * from {{ ref('stg_pc_worknode') }}
),

-- Mapping current worknode from opportunity_applicant logic (reused from int_pc_credit_data)
worknode_mapping as (
    select
        a.user_id,
        string_agg(distinct w_center.worknode_name, ',') as current_work_node_name,
        string_agg(distinct w_center.worknode_type, ',') as current_work_node_type
    from {{ ref('stg_pc_opportunity_applicant') }} a
    left join worknode w_center on a.applied_to_entity_id = w_center.worknode_id
    group by 1
)

select
    r.user_id,
    u.first_name || ' ' || coalesce(u.last_name, '') as full_name,
    r.retention_id,
    rl.current_roles,
    r.retention_status,
    u.login as email_address,
    -- Mobile number mapping would require personContacts join if needed
    cast(null as text) as mobile_number, 
    f.preferred_role,
    r.retention_year,
    u.updated_datetime as user_updated_date_time,
    f.willing_to_continue,
    wm.current_work_node_name,
    wm.current_work_node_type,
    w_pref.worknode_name as preferred_work_node_name,
    w_pref.worknode_type as preferred_work_node_type,
    r.retention_sent_date as retention_sent_date_time,
    cast(null as text) as reasons_for_not_continuing -- Requires bridge table join if needed
from retention r
left join form f on r.retention_id = f.retention_id
left join users u on r.user_id = u.user_id
left join roles rl on r.user_id = rl.user_id
left join worknode_mapping wm on r.user_id = wm.user_id
left join worknode w_pref on f.preferred_work_node_id = w_pref.worknode_id
