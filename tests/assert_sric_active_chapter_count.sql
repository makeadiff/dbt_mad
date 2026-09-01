-- §6.6: chapter_status = true must require BOTH dim_chapter_current_status.is_currently_active
-- AND int_bubble__partner.converted (MOU signed) -- is_currently_active alone let 4 unconverted
-- chapters through, inflating the active count from 68 to 72. Fails if a chapter is marked
-- chapter_status = true in the model output without being converted in Bubble.
select
    s.chapter_id,
    s.chapter_status,
    bp.converted
from {{ ref('prod_sric_dashboard_data') }} s
inner join {{ ref('dim_bubble_partner') }} p on s.chapter_id::integer = p.bubble_partner_id::integer
left join {{ ref('int_bubble__partner') }} bp on p.bubble_partner_id = bp.partner_id1
where s.chapter_status = true
  and coalesce(bp.converted, false) = false
