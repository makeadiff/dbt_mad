{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per chapter per cancellation reason, sourced from
-- prod_do_cancellation_reasons; the reason is the school holiday reason as logged in Bubble.

select
    chapter_name                    as "Chapter",
    city                            as "City",
    cho_name                        as "Chapter organiser",
    district                        as "District",
    do_name                         as "DO",
    cancel_reason                   as "Reason",
    sessions_cancelled              as "Sessions cancelled"
from {{ ref('prod_do_cancellation_reasons') }}
