with qty_merge as (
	select 
		a.quantity
		, a.ptype_id
		, a.running_total as upper_prob
		, zeroifnull(b.running_total) as lower_prob
	from new_qty_prob_curve as a
	left join new_qty_prob_curve as b
		on a.quantity = (b.quantity + 1) and a.ptype_id = b.ptype_id
),
current_price_date as (
	select
		p.prod_id as product_id
		, max(eff_date) as price_date
	from dim_pricing as p
	where eff_date <= '{{ var("gen_date") }}'
	group by p.prod_id
),
days_out as (
	select 
		a.days_to_start
		, a.running_total as upper_prob
		, zeroifnull(b.running_total) as lower_prob
	from days_to_start_curve as a
	left join days_to_start_curve as b
		on a.days_to_start = (b.days_to_start - 1)
)
select
	n.customer_id as account_id
	, '{{ var("gen_date") }}' as contract_date
	, n.product_id
	, q.quantity as quantity
	, q.quantity * p.unit_price as contract_amount
	, dateadd('day', do.days_to_start, contract_date)::date as start_date
	, dateadd('year', 1, start_date) as end_date
from {{ ref("new_calc") }} as n
left join dim_products as pr
	on pr.product_id = n.product_id
left join qty_merge as q
	on n.random_seed2 <= q.upper_prob and n.random_seed2 > q.lower_prob and q.ptype_id = pr.ptype_id
left join current_price_date as cpd
	on cpd.product_id = n.product_id
left join dim_pricing as p
	on quantity <= p.max_qty and quantity >= p.min_qty and cpd.price_date = p.eff_date and n.product_id = cpd.product_id
left join days_out as do
	on n.random_seed3 <= do.upper_prob and n.random_seed3 > do.lower_prob