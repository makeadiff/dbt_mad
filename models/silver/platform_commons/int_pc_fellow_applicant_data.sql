{{ config(materialized='table') }}

with applicant as (
    select * from {{ ref('stg_pc_opportunity_applicant') }}
),

users as (
    select * from {{ ref('stg_pc_user') }}
),

person as (
    select * from {{ ref('stg_pc_person') }}
),

profile as (
    select * from {{ ref('stg_pc_person_profile') }}
),

gender as (
    select * from {{ ref('stg_pc_gender') }}
),

worknode as (
    select * from {{ ref('stg_pc_worknode') }}
),

roles as (
    select
        user_id,
        string_agg(distinct replace(role_code, 'role.mad.', ''), ',') as roles_played_in_mad,
        string_agg(distinct case when is_deleted is not true then replace(role_code, 'role.mad.', '') end, ',') as current_roles_assigned
    from {{ ref('stg_pc_user_role') }}
    where role_code = lower(role_code)
      and role_code not like '%user.administrator%'
    group by 1
),

meta as (
    select * from {{ ref('stg_pc_opportunity_applicant_meta') }}
),

total_work_exp as (
    select * from {{ ref('stg_pc_total_work_experience') }}
),

-- Contact chain: person → person_personContacts_bridge → personContacts → contact
-- Reuses the same proven pattern from int_pc_user_data
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
),

person_whatsapp_deduped as (
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
    where c.contact_type = 'CONTACT_TYPE.WHATSAPP_NUMBER'
),

workforce_selection as (
    select
        user_id,
        worknode_id,
        row_number() over (
            partition by user_id 
            order by 
                case when is_active = true then 1 else 2 end,
                workforce_id desc
        ) as rn
    from {{ ref('stg_pc_workforce') }}
),

-- Aggregate current work nodes per user from opportunity_applicant → worknode
worknode_assigned as (
    select
        oa.user_id,
        string_agg(distinct w.worknode_name, ',') as current_work_nodes
    from {{ ref('stg_pc_opportunity_applicant') }} oa
    left join {{ ref('stg_pc_worknode') }} w 
        on oa.applied_to_entity_id = w.worknode_id
    where oa.user_id is not null
    group by 1
)

select
    g.gender_label as gender,
    a.user_id,
    a.opportunity_applicant_id as applicant_id,
    {{ clean_prefix('a.current_step_code') }} as current_step,
    a.date_of_joining as joining_date,
    u.first_name || ' ' || coalesce(u.last_name, '') as applicant_name,
    a.applicant_medium as sourced_medium,
    a.applicant_source as sourced_source,
    u.login as applicant_email,
    u.created_datetime as user_sign_up_date,
    coalesce(
        m.current_user_type,
        case 
            when r.current_roles_assigned like '%alumni%' then 'Alumni'
            when r.current_roles_assigned is not null then 'MADster'
            else 'Applicant'
        end
    ) as current_user_type,
    a.applicant_campaign as sourced_campaign,
    r.roles_played_in_mad as roles_played_in_mad,
    {{ clean_prefix('a.application_status') }} as application_status,
    {{ clean_prefix('a.current_step_status') }} as current_step_status,
    a.application_datetime,
    u.updated_datetime as user_updated_date_time,
    r.current_roles_assigned as current_roles_assigned,
    pcd.contact_value as primary_contact_number,
    w_applied.worknode_name as applied_to_work_node_name,
    {{ clean_prefix('a.applied_to_entity_type') }} as applied_to_work_node_type,
    pwd.contact_value as whatsapp_contact_number,
    case
        when twe.experience_value = 0 then '0'
        when twe.experience_value > 0 and twe.experience_value < 1 then '0-1 year'
        when twe.experience_value >= 1 and twe.experience_value < 2 then '1-2 year'
        when twe.experience_value >= 2 and twe.experience_value < 3 then '2-3 year'
        when twe.experience_value >= 3 and twe.experience_value < 5 then '3-5 year'
        when twe.experience_value >= 5 then '5+ years'
        else null
    end as total_years_of_experience,
    cast(null as integer) as numbers_of_feedbacks_given, -- Not stored in pc_raw, computed field
    w_sel_resolved.worknode_name as selected_for_work_node_name,
    {{ clean_prefix('w_sel_resolved.worknode_type') }} as selected_for_work_node_type,
    wa.current_work_nodes as current_work_nodes_assigned,
    w_parent.worknode_name as selected_for_parent_work_node,
    {{ clean_prefix('w_parent.worknode_type') }} as selected_for_parent_work_node_type
from applicant a
join users u on a.user_id = u.user_id
left join roles r on a.user_id = r.user_id
left join person p on u.person_id = p.person_id
left join profile pr on p.person_profile_id = pr.person_profile_id
left join gender g on pr.gender_data_code = g.gender_data_code
left join worknode w_applied on a.applied_to_entity_id = w_applied.worknode_id
left join worknode w_selected on a.secondary_applied_to_worknode_id = w_selected.worknode_id
left join workforce_selection wf_sel on a.user_id = wf_sel.user_id and wf_sel.rn = 1
left join worknode w_sel_resolved on coalesce(w_selected.worknode_id, wf_sel.worknode_id) = w_sel_resolved.worknode_id
left join {{ ref('stg_pc_worknode_hierarchy') }} wh on w_sel_resolved.worknode_id = wh.worknode_id and wh.depth = 1 and wh.is_active = true
left join worknode w_parent on wh.parent_worknode_id = w_parent.worknode_id
left join meta m on a.opportunity_applicant_id = m.opportunity_applicant_id
left join total_work_exp twe on pr.total_work_experience_id = twe.total_work_experience_id
left join person_contact_deduped pcd on p.person_id = pcd.person_id and pcd.rn = 1
left join person_whatsapp_deduped pwd on p.person_id = pwd.person_id and pwd.rn = 1
left join worknode_assigned wa on a.user_id = wa.user_id
where a.for_role in ('role.mad.fellow', 'role.mad.city_team_lead_fellow')
