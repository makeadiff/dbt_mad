{% macro role_code_to_label(column_name) %}
    case {{ column_name }}
        when 'role.mad.project_lead' then 'Project Lead'
        when 'role.mad.city_team_lead_fellow' then 'CHO'
        when 'role.mad.youth' then 'Youth'
        when 'role.mad.community_organiser' then 'CO Full Time'
        when 'role.mad.academic_support' then 'Academic Support'
        when 'role.mad.function_lead' then 'Function Lead'
        when 'role.mad.wingman' then 'Wingman'
        when 'role.mad.fellow' then 'Fellow'
        when 'role.mad.community_organiser_part_time' then 'CO Part Time'
        when 'role.mad.applicant' then 'Applicant'
        when 'role.mad.project_associate' then 'Project Associate'
        when 'role.mad.admin' then 'Admin'
        when 'role.mad.alumni' then 'Alumni'
        else {{ column_name }}
    end
{% endmacro %}
