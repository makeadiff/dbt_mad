{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'address') }}
)

select
    a.id::bigint as address_id,
    a."addressLine1"::text as address_line_1,
    a."addressLine2"::text as address_line_2,
    coalesce(c.city_name, a."cityDataCode")::text as city,
    coalesce(s.state_name, a."stateDataCode")::text as state,
    a."countryDataCode"::text as country,
    a."pinCode"::text as pincode,
    a."isActive"::boolean as is_active,
    a."createdDateTime"::timestamp as created_datetime,
    a."updatedDateTime"::timestamp as updated_datetime,
    a."xIsDeleted"::text as is_deleted
from raw a
left join {{ ref('stg_pc_city') }} c on a."cityDataCode"::text = c.city_data_code and c.rn = '1'
left join {{ ref('stg_pc_state') }} s on a."stateDataCode"::text = s.state_data_code and s.rn = '1'
where a."xIsDeleted" is false or a."xIsDeleted" is null
