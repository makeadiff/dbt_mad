{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. One row per CCI per cancellation reason, sourced from
-- prod_cci_cancellation_reasons (spec E1 §3.10); coded reasons read "Reason code N".

select
    chapter_name                    as "Chapter",
    city                            as "City",
    district                        as "District",
    do_name                         as "DO",
    cancel_reason                   as "Reason",
    sessions_cancelled              as "Sessions cancelled"
from {{ ref('prod_cci_cancellation_reasons') }}
