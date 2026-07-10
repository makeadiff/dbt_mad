{{ config(materialized='table') }}

SELECT
    "City" AS city,
    "State" AS state,
    "Center" AS center,
    "Target_F" AS target_f,
    "Target_M" AS target_m,
    "Target_O" AS target_o,
    "Sourcing_F" AS sourcing_f,
    "Sourcing_M" AS sourcing_m,
    "Sourcing_O" AS sourcing_o,
    "Allocated_F" AS allocated_f,
    "Allocated_M" AS allocated_m,
    "Allocated_O" AS allocated_o,
    "Requirement_F" AS requirement_f,
    "Requirement_M" AS requirement_m,
    "Requirement_O" AS requirement_o

FROM {{ source('source_platform_commons', 'target_settings_int') }}
