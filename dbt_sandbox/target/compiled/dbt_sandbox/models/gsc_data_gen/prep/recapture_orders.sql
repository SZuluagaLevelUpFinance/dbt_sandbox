with  __dbt__cte__recapture_calc as (
/*
	Identify churned customers and calculate if they rejoin.
*/

with last_end as (
select
	o.account_id
	, o.product_id
	, max(o.end_date) as last_end
from orders as o
group by o.account_id, o.product_id
)
select
	le.account_id
	, le.product_id
	, le.last_end
	, datediff('day', le.last_end, '2025-08-08') as days_since_exp
	, rp.recap_prob * 0.01 as act_recap_prob
	, uniform(0::float, 100::float, random()) as random_seed
	, case 
		when random_seed <= act_recap_prob then
			true
		else
			false
		end as recap_flag
	, uniform(0::float, 100::float, random()) as random_seed2
	, uniform(0::float, 100::float, random()) as random_seed3
from last_end as le
left join recap_prob_curve as rp
	on days_since_exp <= rp.max_days
		and days_since_exp >= rp.min_days
where le.last_end < '2025-08-08'
	and recap_flag = true
), qty_merge as (
	select 
		a.quantity as qty
		, a.ptype_id
		, a.running_total as upper_prob
		, zeroifnull(b.running_total) as lower_prob
	from recap_qty_prob_curve as a
	left join recap_qty_prob_curve as b
		on a.quantity = (b.quantity + 1) and a.ptype_id = b.ptype_id
),
current_price_date as (
	select
		prod_id
		, max(eff_date) as price_date
	from dim_pricing 
	where eff_date <= '2025-08-08'
	group by prod_id
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
	r.account_id
	, r.product_id
	, q.qty as quantity
	, q.qty * p.unit_price as contract_amount
	, '2025-08-08' as contract_date
	, dateadd('day', do.days_to_start, contract_date)::date as start_date
	, dateadd('year', 1, start_date) as end_date
from __dbt__cte__recapture_calc as r
left join dim_products as pr
	on pr.product_id = r.product_id
left join qty_merge as q
	on r.random_seed2 <= q.upper_prob and r.random_seed2 > q.lower_prob and q.ptype_id = pr.ptype_id
left join current_price_date as cpd
	on cpd.prod_id = r.product_id
left join dim_pricing as p
	on quantity <= p.max_qty and quantity >= p.min_qty and cpd.price_date = p.eff_date
left join days_out as do
	on r.random_seed3 <= do.upper_prob and r.random_seed3 > do.lower_prob