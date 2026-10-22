{{ config(materialized='table') }}

-- Dalgo-facing presentation model. Column names are display names by design. Nothing in dbt
-- should ref() this model. "What the words mean" table (spec §3.6c), one row per term, from
-- seed_do_glossary. The Dalgo table sorts on "Order" and hides it.

select
    term            as "Term",
    definition      as "What it means",
    term_order      as "Order"
from {{ ref('seed_do_glossary') }}
