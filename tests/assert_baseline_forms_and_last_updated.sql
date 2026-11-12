-- Baseline forms submitted (DO spec §8, 2026-11-12): per chapter, forms submitted >= children with
-- that form done, for each form. And the "Last updated" tables have their fixed rows: 2 on the
-- E2 dashboard (DOTS, Bubble), 1 on the E1 dashboard (Platform Commons).
select 'forms < children: ' || chapter_id as failed_check
from {{ ref('prod_do_baseline_chapters') }}
where forms_numeracy_submitted < children_numeracy_done
   or forms_literacy_submitted < children_literacy_done
   or forms_life_skills_submitted < children_life_skills_done
union all
select 'prod_do_last_updated_display rows'
from (select count(*) n from {{ ref('prod_do_last_updated_display') }}) c where n <> 2
union all
select 'prod_cci_last_updated_display rows'
from (select count(*) n from {{ ref('prod_cci_last_updated_display') }}) c where n <> 1
