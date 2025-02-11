/*
	Finalize renewal orders in the column format required.
*/

with 
current_price_date as (
	select
		prod_id as product_id
		, max(eff_date) as price_date
	from dim_pricing 
	where eff_date <= '{{ var("gen_date") }}'
	group by product_id
),
up_merge as (
	select 
		a.quantity
		, a.ptype_id
		, a.up_running as upper_prob
		, zeroifnull(b.up_running) as lower_prob
	from up_at_ren_prob_curve as a
	left join up_at_ren_prob_curve as b
		on a.quantity = (b.quantity + 1) and a.ptype_id = b.ptype_id
),
down_merge as (
	select 
		a.quantity
		, a.ptype_id
		, a.down_running as upper_prob
		, zeroifnull(b.down_running) as lower_prob
	from down_at_ren_prob_curve as a
	left join down_at_ren_prob_curve as b
		on a.quantity = (b.quantity + 1) and a.ptype_id = b.ptype_id
)
select 
	r.account_id
	, '{{ var("gen_date") }}' as contract_date
	, r.product_id
	, r.end_date as start_date
	, dateadd('year', 1, r.end_date) as end_date
	, r.active_quantity + 
		case
			when upsell_at_ren = true then 
				u.quantity
			when downsell_at_ren = true then 
				case
					when d.quantity >= r.active_quantity then 
						1 - r.active_quantity
					else 
						d.quantity * -1
					end
			when flat_renewal = true then 
				0
			end as qty
	, qty * p.unit_price as contract_amount
from {{ ref('renewals_updown') }} as r
left join dim_products as pr 
	on pr.product_id = r.product_id
left join up_merge as u
	on r.random_seed2 <= u.upper_prob and r.random_seed2 > u.lower_prob and u.ptype_id = pr.ptype_id
left join down_merge as d
	on r.random_seed2 <= d.upper_prob and r.random_seed2 > d.lower_prob and d.ptype_id = pr.ptype_id
left join current_price_date as cpd
	on cpd.product_id = r.product_id
left join dim_pricing as p
	on qty <= p.max_qty and qty >= p.min_qty and cpd.price_date = p.eff_date and r.product_id = p.prod_id