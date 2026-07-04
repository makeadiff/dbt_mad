{{ config(materialized='table') }}

with school as (
    select * from {{ ref('stg_pc_school') }}
)

select
    s.school_id,
    s.is_active,
    s.updated_datetime as user_updated_date_time
from school s
