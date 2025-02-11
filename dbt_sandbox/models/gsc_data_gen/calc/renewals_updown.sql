/*
	Establish whether or not a renewal will upsell or downsell.
*/

select
	r.account_id 
	, r.product_id 
	, r.end_date 
	, r.active_quantity 
	, r.renewed_flag
	, uniform(0::float, 100::float, random()) as random_seed 
	, case
		when random_seed <= {{ var('renewal_upsell_prob') }} then
			true
		else 
			false
		end as upsell_at_ren
	, case
		when upsell_at_ren = true then
			false
		when active_quantity = 1 then
			false
		when random_seed >= (100 - {{ var('renewal_downsell_prob') }}) then
			true
		else
			false
		end as downsell_at_ren
	, case 
		when upsell_at_ren = false and downsell_at_ren = false 
			then true 
		else 
			false 
		end as flat_renewal
	, uniform(0::float, 100::float, random()) as random_seed2
from {{ ref('renewals_calc') }} as r