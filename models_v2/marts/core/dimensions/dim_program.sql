{{ config(materialized='table') }}

-- dim_program: One row per program
-- Flow: stg_bubble__program → dim_program

select
    program_id,
    program_name,
    created_date,
    modified_date
from {{ ref('stg_bubble__program') }}
