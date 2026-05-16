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
        string_agg(distinct role_code, ',') as roles_assigned
    from {{ ref('stg_pc_user_role') }}
    group by 1
)

select
    g.gender_label as gender,
    a.user_id,
    a.opportunity_applicant_id as applicant_id,
    a.current_step_code as current_step,
    cast(null as timestamp) as joining_date, -- Mapping date of joining if available
    u.first_name || ' ' || coalesce(u.last_name, '') as applicant_name,
    a.applicant_medium as sourced_medium,
    a.applicant_source as sourced_source,
    u.login as applicant_email,
    u.created_datetime as user_sign_up_date,
    cast(null as text) as current_user_type,
    a.applicant_campaign as sourced_campaign,
    r.roles_assigned as roles_played_in_mad,
    a.application_status,
    a.current_step_status,
    a.application_datetime,
    u.updated_datetime as user_updated_date_time,
    r.roles_assigned as current_roles_assigned,
    cast(null as text) as primary_contact_number,
    w_applied.worknode_name as applied_to_work_node_name,
    w_applied.worknode_type as applied_to_work_node_type,
    cast(null as text) as whatsapp_contact_number,
    cast(null as text) as total_years_of_experience,
    cast(null as integer) as numbers_of_feedbacks_given,
    w_selected.worknode_name as selected_for_work_node_name,
    w_selected.worknode_type as selected_for_work_node_type,
    cast(null as text) as current_work_nodes_assigned,
    w_parent.worknode_name as selected_for_parent_work_node,
    w_parent.worknode_type as selected_for_parent_work_node_type
from applicant a
join users u on a.user_id = u.user_id
left join roles r on a.user_id = r.user_id
left join person p on u.person_id = p.person_id
left join profile pr on p.person_profile_id = pr.person_profile_id
left join gender g on pr.gender_data_code = g.gender_data_code
left join worknode w_applied on a.applied_to_entity_id = w_applied.worknode_id
left join worknode w_selected on a.secondary_applied_to_worknode_id = w_selected.worknode_id
left join worknode w_parent on w_selected.linked_system_id = w_parent.worknode_id -- Assuming hierarchy via linkedSystemId for now
where a.for_role in ('role.mad.fellow', 'role.mad.city_team_lead_fellow')
