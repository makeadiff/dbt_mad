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

person as (
    select * from {{ ref('stg_pc_person') }}
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

form_reasons as (
    select
        bridge.retention_form_response_id,
        string_agg(distinct r.reason_value, ', ') as reasons_for_not_continuing
    from {{ ref('stg_pc_retention_form_response_reasons_bridge') }} bridge
    join {{ ref('stg_pc_reasons') }} r on bridge.reason_id = r.reason_id
    group by 1
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
),

-- Contact chain: user → person → person_personContacts_bridge → personContacts → contact
person_contact_deduped as (
    select 
        pcb.person_id,
        c.contact_value,
        row_number() over (
            partition by pcb.person_id 
            order by pc.is_primary desc, c.contact_id desc
        ) as rn
    from {{ ref('stg_pc_person_person_contacts_bridge') }} pcb
    join {{ ref('stg_pc_person_contacts') }} pc 
        on pcb.person_contact_id = pc.person_contact_id
    join {{ ref('stg_pc_contact') }} c 
        on pc.contact_id = c.contact_id
    where c.contact_type in ('CONTACT_TYPE.MOBILE', 'MOBILE')
)

select
    r.user_id,
    u.first_name || ' ' || coalesce(u.last_name, '') as full_name,
    r.retention_id,
    rl.current_roles,
    r.retention_status,
    u.login as email_address,
    pcd.contact_value as mobile_number,
    f.preferred_role,
    r.retention_year,
    u.updated_datetime as user_updated_date_time,
    f.willing_to_continue,
    wm.current_work_node_name,
    wm.current_work_node_type,
    w_pref.worknode_name as preferred_work_node_name,
    w_pref.worknode_type as preferred_work_node_type,
    r.retention_sent_date as retention_sent_date_time,
    fr.reasons_for_not_continuing
from retention r
left join form f on r.retention_id = f.retention_id
left join users u on r.user_id = u.user_id
left join person ps on u.person_id = ps.person_id
left join roles rl on r.user_id = rl.user_id
left join worknode_mapping wm on r.user_id = wm.user_id
left join worknode w_pref on f.preferred_work_node_id = w_pref.worknode_id
left join person_contact_deduped pcd on ps.person_id = pcd.person_id and pcd.rn = 1
left join form_reasons fr on f.retention_form_response_id = fr.retention_form_response_id

