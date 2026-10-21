-- prod_do_national_kpis is a single national row; anything else breaks the Dalgo tiles.
select count(*) as n
from {{ ref('prod_do_national_kpis') }}
having count(*) <> 1
