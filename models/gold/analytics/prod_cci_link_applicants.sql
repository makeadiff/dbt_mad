{{ config(materialized='table') }}

-- CCI dashboard (E1): this year's volunteer applicants who applied through a CCI's sourcing link
-- (spec E1 §7 L4). Shared by prod_cci_chapter_summary (leads from this chapter's link, city leads
-- less these) and prod_cci_national_display, so the attribution lives in one place. Not read by Dalgo.
--
-- Link: the application's applicant_campaign equals the CCI's sourcing_campaign_code on the chapter
-- mapping sheet (int_google_sheet__chapter_mapping, engine E1; chapter_id there is the Platform
-- Commons school_id). Codes and campaigns are btrimmed of spaces, tabs and newlines: two sheet codes
-- carry a leading tab, which trim() leaves. Only the 2026-27 volunteer-intake opportunity
-- (seed_pc_opportunity_academic_year). fct_volunteer_pipeline only joins E2 codes (its scope), so it
-- files these applicants as 'city' or 'chapter_unmatched'; that shared fact is left unchanged.
--
-- Grain: one row per (volunteer_id, chapter_id); a volunteer who used two CCI links is on both.

with cci_codes as (
    select m.chapter_id, btrim(m.sourcing_campaign_code, E' \t\r\n') as campaign_code
    from {{ ref('int_google_sheet__chapter_mapping') }} m
    join {{ ref('seed_cci_chapter_group') }} g
        on m.chapter_id = g.school_id::text
    where m.engine = 'E1'
      and nullif(btrim(m.sourcing_campaign_code, E' \t\r\n'), '') is not null
)

select distinct
    a.user_id::numeric as volunteer_id,
    cc.chapter_id
from {{ ref('stg_pc_opportunity_applicant') }} a
join {{ ref('seed_pc_opportunity_academic_year') }} oy
    on a.opportunity_id = oy.opportunity_id
   and oy.academic_year = '2026-27'
   and oy.is_volunteer_intake
join cci_codes cc
    on btrim(a.applicant_campaign, E' \t\r\n') = cc.campaign_code
where a.is_deleted = false
  and a.user_id is not null
