/* 
	Calculate actual addon quantity and prepare for union.
*/

with  __dbt__cte__addon_calc as (
/*
	Find subscriptions that are eligible to addon, and then calculate whether or not they actually addon.
*/

with active_subs as (
	select
		o.account_id
		, o.product_id
		, o.end_date
		, sum(o.quantity) as active_quantity
	from orders as o
	where
		o.end_date >= '2025-08-08'
		and o.start_date < '2025-08-08'
	group by
		o.account_id
		, o.product_id 
		, o.end_date
),
eligible_to_addon as (
	select 
		e.account_id
		, e.product_id
		, e.end_date
		, e.active_quantity
		, o2.contract_id
		, o2.start_date
		, case 
			when e.end_date = o2.start_date then
				true
			else 
				false
			end as pending_renewal
		, case
			when o2.start_date is not null and e.end_date <> o2.start_date then 
				true 
			else 
				false 
			end as pending_addon
	from active_subs as e
	left join orders as o2
		on o2.account_id = e.account_id 
		and o2.product_id = e.product_id 
		and o2.start_date >= '2025-08-08'
	where 
			pending_renewal = false 
		and pending_addon = false
		and e.end_date > '2025-08-08'
)
select
	e.account_id
	, e.product_id
	, e.end_date
	, e.active_quantity
--	, .2 as addon_daily_prob
	, uniform(0::float, 100::float, random()) as random_seed
	, case 
		when random_seed <= 0.05 then 
			true 
		else 
			false
		end as addon_flag
	, uniform(0::float, 100::float, random()) as random_seed2
from eligible_to_addon as e
where addon_flag = true
), qty_merge as (
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
	where eff_date <= '2025-08-08'
	group by product_id
)
select 
	a.account_id
	, a.product_id
	, '2025-08-08' as contract_date
	, '2025-08-08' as start_date
	, a.end_date
	, q.qty as addon_quantity
	, a.active_quantity + q.qty as pricing_quantity
	, round(p.unit_price * addon_quantity * datediff('day', start_date, a.end_date) / 365, 2) as contract_amount
from __dbt__cte__addon_calc as a
left join dim_products as pr
	on pr.product_id = a.product_id
left join qty_merge as q
	on a.random_seed2 <= upper_prob and a.random_seed2 > lower_prob and q.ptype_id = pr.ptype_id
left join current_price_date as cpd
	on cpd.product_id = a.product_id
left join dim_pricing as p
	on pricing_quantity <= p.max_qty and pricing_quantity >= p.min_qty and cpd.price_date = p.eff_date and a.product_id = p.prod_id