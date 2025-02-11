/* 
	Calculate actual addon quantity and prepare for union.
*/

with qty_merge as (
	select 
		a.quantity as qty
		, a.ptype_id
		, a.up_running as upper_prob
		, zeroifnull(b.up_running) as lower_prob
	from addon_prob_curve as a
	left join addon_prob_curve as b
		on a.quantity = (b.quantity + 1) and a.ptype_id = b.ptype_id
),
current_price_date as (
	select
		prod_id as product_id
		, max(eff_date) as price_date
	from dim_pricing 
	where eff_date <= '{{ var("gen_date") }}'
	group by product_id
)
select 
	a.account_id
	, a.product_id
	, '{{ var("gen_date") }}' as contract_date
	, '{{ var("gen_date") }}' as start_date
	, a.end_date
	, q.qty as addon_quantity
	, a.active_quantity + q.qty as pricing_quantity
	, round(p.unit_price * addon_quantity * datediff('day', start_date, a.end_date) / 365, 2) as contract_amount
from {{ ref("addon_calc") }} as a
left join dim_products as pr
	on pr.product_id = a.product_id
left join qty_merge as q
	on a.random_seed2 <= upper_prob and a.random_seed2 > lower_prob and q.ptype_id = pr.ptype_id
left join current_price_date as cpd
	on cpd.product_id = a.product_id
left join dim_pricing as p
	on pricing_quantity <= p.max_qty and pricing_quantity >= p.min_qty and cpd.price_date = p.eff_date and a.product_id = p.prod_id