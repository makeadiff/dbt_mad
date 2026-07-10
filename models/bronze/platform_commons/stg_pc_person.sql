{{ config(materialized='table') }}

with raw as (
    select * from {{ source('pc_raw', 'person') }}
)

select
    id::bigint as person_id,
    "personProfileId"::bigint as person_profile_id,
    "isActive"::boolean as is_active,
    "xIsDeleted"::boolean as is_deleted
from raw
where "xIsDeleted" is false or "xIsDeleted" is null
