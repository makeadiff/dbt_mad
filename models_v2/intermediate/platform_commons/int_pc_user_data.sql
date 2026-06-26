{{ config(
  materialized='table'
) }}

with mad_roles as (
    select
        user_id,
        role_code,
        row_number() over (partition by user_id order by user_role_id desc) as rn
    from {{ ref('stg_pc_user_role') }}
    where (is_deleted is false or is_deleted is null)
    and role_code ilike 'role.mad.%'
    and role_code != 'role.mad.applicant'
),

users as (
  select u.* 
  from {{ ref('stg_pc_user') }} u
  inner join mad_roles r on u.user_id = r.user_id and r.rn = 1
),

person_address_deduped as (
    select 
        pab.person_id,
        coalesce(c.city_name, a."cityDataCode") as city,
        coalesce(s.state_name, a."stateDataCode") as state,
        row_number() over (partition by pab.person_id order by a."updatedDateTime" desc, a.id desc) as rn
    from {{ ref('stg_pc_person_person_addresses_bridge') }} pab
    join {{ source('pc_raw', 'personAddresses') }} pa on pab.person_address_id = pa.id
    join {{ source('pc_raw', 'address') }} a on pa."addressId" = a.id
    left join {{ ref('stg_pc_city') }} c on a."cityDataCode" = c.city_data_code and c.rn = '1'
    left join {{ ref('stg_pc_state') }} s on a."stateDataCode" = s.state_data_code and s.rn = '1'
    where pa."isActive" = true
),

person_contact_deduped as (
    select 
        pcb.person_id,
        c.contact_value,
        row_number() over (partition by pcb.person_id order by pc.is_primary desc, c.contact_id desc) as rn
    from {{ ref('stg_pc_person_person_contacts_bridge') }} pcb
    join {{ ref('stg_pc_person_contacts') }} pc on pcb.person_contact_id = pc.person_contact_id
    join {{ ref('stg_pc_contact') }} c on pc.contact_id = c.contact_id
    where c.contact_type in ('CONTACT_TYPE.MOBILE', 'MOBILE')
),

-- Mapping current worknode from opportunity_applicant logic
worknode_mapping as (
    select
        a.user_id,
        w_center.worknode_name as center,
        w_city.worknode_name as city,
        w_state.worknode_name as state,
        row_number() over (partition by a.user_id order by a.application_datetime desc) as rn
    from {{ ref('stg_pc_opportunity_applicant') }} a
    left join {{ ref('stg_pc_worknode') }} w_center on a.applied_to_entity_id = w_center.worknode_id
    left join {{ ref('stg_pc_worknode') }} w_city on a.secondary_applied_to_worknode_id = w_city.worknode_id
    left join {{ ref('stg_pc_worknode') }} w_state on w_city.linked_system_id = w_state.worknode_id
    where a.user_id is not null
),

hierarchy as (
    select
        ur_child.user_id,
        ur_parent.user_id as reporting_manager_user_id,
        ur_parent.role_code as reporting_manager_role_code,
        u_parent.login as reporting_manager_user_login,
        row_number() over (partition by ur_child.user_id order by urh.user_role_hierarchy_id desc) as rn
    from {{ ref('stg_pc_user_role_hierarchy') }} urh
    join {{ ref('stg_pc_user_role') }} ur_child on urh.user_role_id = ur_child.user_role_id
    join {{ ref('stg_pc_user_role') }} ur_parent on urh.parent_user_role_id = ur_parent.user_role_id
    join {{ ref('stg_pc_user') }} u_parent on ur_parent.user_id = u_parent.user_id
    where urh.is_active = true
)

select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as user_key,

    coalesce(pa.city, w.city) as "City",
    u.login as "Email",
    coalesce(pa.state, w.state) as "State",
    w.center as "Center",
    u.user_id as "UserId",
    cast(null as text) as "AddedBy",
    c.contact_value as "Contact",
    r.role_code as "UserRole",
    u.login as "UserLogin",
    u.first_name || ' ' || coalesce(u.last_name, '') as "UserDisplayName",
    
    -- Reporting Manager fields
    h.reporting_manager_user_id as "ReportingManagerUserId",
    h.reporting_manager_role_code as "ReportingManagerRoleCode",
    h.reporting_manager_user_login as "ReportingManagerUserLogin",
    
    u.created_datetime as "UserCreatedDateTime",
    u.updated_datetime as "UserUpdatedDateTime"

from users u
left join mad_roles r on u.user_id = r.user_id and r.rn = 1
left join person_address_deduped pa on u.person_id = pa.person_id and pa.rn = 1
left join person_contact_deduped c on u.person_id = c.person_id and c.rn = 1
left join worknode_mapping w on u.user_id = w.user_id and w.rn = 1
left join hierarchy h on u.user_id = h.user_id and h.rn = 1
