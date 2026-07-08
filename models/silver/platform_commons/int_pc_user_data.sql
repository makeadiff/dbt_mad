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

-- Highest-priority role per user (mirrors raw view's role hierarchy ranking), used for display only
role_priority as (
    select
        user_id,
        role_code,
        row_number() over (
            partition by user_id
            order by
                case role_code
                    when 'role.mad.admin' then 10
                    when 'role.mad.function_lead' then 9
                    when 'role.mad.project_lead' then 8
                    when 'role.mad.project_associate' then 7
                    when 'role.mad.community_organiser' then 6
                    when 'role.mad.community_organiser_part_time' then 6
                    when 'role.mad.city_team_lead_fellow' then 6
                    when 'role.mad.fellow' then 5
                    when 'role.mad.wingman' then 4
                    when 'role.mad.academic_support' then 4
                    when 'role.mad.youth' then 3
                    when 'role.mad.applicant' then 2
                    when 'role.mad.alumni' then 1
                    else 0
                end desc,
                modified_datetime desc
        ) as rn
    from {{ ref('stg_pc_user_role') }}
    where (is_deleted is false or is_deleted is null)
    and role_code in (
        'role.mad.admin', 'role.mad.function_lead', 'role.mad.project_lead',
        'role.mad.project_associate', 'role.mad.community_organiser',
        'role.mad.community_organiser_part_time', 'role.mad.city_team_lead_fellow',
        'role.mad.fellow', 'role.mad.wingman', 'role.mad.academic_support',
        'role.mad.youth', 'role.mad.applicant', 'role.mad.alumni'
    )
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
        row_number() over (partition by pab.person_id order by pa.id desc) as rn
    from {{ ref('stg_pc_person_person_addresses_bridge') }} pab
    join {{ source('pc_raw', 'personAddresses') }} pa on pab.person_address_id = pa.id
    join {{ source('pc_raw', 'address') }} a on pa."addressId" = a.id
    left join {{ ref('stg_pc_city') }} c on a."cityDataCode" = c.city_data_code and c.rn = '1'
    left join {{ ref('stg_pc_state') }} s on a."stateDataCode" = s.state_data_code and s.rn = '1'
),

-- Aggregated contact fields per person (mobile + mail), mirrors raw view's group-by approach
person_contact_agg as (
    select
        pcb.person_id,
        max(case when c.contact_type in ('CONTACT_TYPE.MOBILE', 'MOBILE') then c.contact_value end) as contact,
        max(case when c.contact_type in ('CONTACT_TYPE.MAIL', 'MAIL') then c.contact_value end) as email
    from {{ ref('stg_pc_person_person_contacts_bridge') }} pcb
    join {{ ref('stg_pc_person_contacts') }} pc on pcb.person_contact_id = pc.person_contact_id
    join {{ ref('stg_pc_contact') }} c on pc.contact_id = c.contact_id
    group by pcb.person_id
),

-- Current workforce allocation (mirrors raw view's workforce -> worknode lookup)
workforce_mapping as (
    select
        wf.user_id,
        wn.worknode_name as center,
        row_number() over (partition by wf.user_id order by wf.modified_datetime desc) as rn
    from {{ ref('stg_pc_workforce') }} wf
    left join {{ ref('stg_pc_worknode') }} wn on wf.worknode_id = wn.worknode_id
),

-- Mapping AddedBy from opportunity_applicant logic
added_by_mapping as (
    select
        a.user_id,
        coalesce(
            u_ref.login,
            u_creator.login,
            'admin'
        ) as added_by,
        row_number() over (partition by a.user_id order by a.application_datetime desc) as rn
    from {{ ref('stg_pc_opportunity_applicant') }} a
    left join {{ ref('stg_pc_opportunity') }} opp on a.opportunity_id = opp.opportunity_id
    left join {{ ref('stg_pc_user') }} u_creator on opp.created_by_user = u_creator.user_id
    left join {{ ref('stg_pc_user') }} u_ref on
        (case when a.applicant_referrer ~ '^[0-9]+$' then a.applicant_referrer::bigint else null end) = u_ref.user_id
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
)

select
    -- Surrogate Key
    {{ dbt_utils.generate_surrogate_key(['u.user_id']) }} as user_key,

    pa.city as "City",
    con.email as "Email",
    pa.state as "State",
    wm.center as "Center",
    u.user_id as "UserId",
    ab.added_by as "AddedBy",
    con.contact as "Contact",
    {{ role_code_to_label('rp.role_code') }} as "UserRole",
    u.login as "UserLogin",
    u.first_name || ' ' || coalesce(u.last_name, '') as "UserDisplayName",

    -- Reporting Manager fields
    h.reporting_manager_user_id as "ReportingManagerUserId",
    {{ role_code_to_label('h.reporting_manager_role_code') }} as "ReportingManagerRoleCode",
    h.reporting_manager_user_login as "ReportingManagerUserLogin",

    u.created_datetime as "UserCreatedDateTime",
    u.modified_datetime as "UserUpdatedDateTime",
    u.is_active as "IsActive"

from users u
left join role_priority rp on u.user_id = rp.user_id and rp.rn = 1
left join person_address_deduped pa on u.person_id = pa.person_id and pa.rn = 1
left join person_contact_agg con on u.person_id = con.person_id
left join workforce_mapping wm on u.user_id = wm.user_id and wm.rn = 1
left join added_by_mapping ab on u.user_id = ab.user_id and ab.rn = 1
left join hierarchy h on u.user_id = h.user_id and h.rn = 1
