-- The freshness display has exactly one row, with a sync date (spec E1 §3.8). Returns a row only
-- when that fails.
select count(*) as row_count
from {{ ref('prod_cci_data_freshness_display') }}
having count(*) <> 1
    or count("Platform Commons last synced") <> 1
