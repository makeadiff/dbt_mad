-- One row, both sync dates present and not in the future (spec §3.6d).
select 1
from (select count(*) as n from {{ ref('prod_do_data_freshness_display') }}) c
where c.n <> 1
union all
select 1
from {{ ref('prod_do_data_freshness_display') }}
where "Session attendance last synced (DOTS)" is null
   or "Volunteers and children last synced (Bubble)" is null
   or "Session attendance last synced (DOTS)" > current_date + 1
   or "Volunteers and children last synced (Bubble)" > current_date + 1
