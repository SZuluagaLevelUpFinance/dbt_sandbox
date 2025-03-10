with  __dbt__cte__new_calc as (
/*
	Figure out which products will have a new customer added.
*/

with launched_products as (
	select 
		p.product_id
		, datediff('month', p.intro_date, '2025-08-08') as months_since_launch
	from dim_products as p
	where p.intro_date <= '2025-08-08'
),
eligible_cust_prod as (
	select distinct
		c.customer_id as last_cust_id
		, lp.product_id
		, row_number() over (order by uniform(0::float, 100::float, random()) desc) as cust_prod_index
	from dim_customers as c
	join launched_products as lp
	left join orders as o
		on o.account_id = c.customer_id and lp.product_id = o.product_id
	where o.contract_id is null
),
cust_prod_map as (
	select
		product_id
		, min(cust_prod_index) as next_cust
	from eligible_cust_prod
	group by product_id
)
select 
	lp.product_id
	, n.new_logos_per_month / date_part('day', last_day('2025-08-08'::date)) * 100 as prob
	, case 
		when lp.months_since_launch > 12 then
			12
		else
			lp.months_since_launch
		end as months_fix	
	, uniform(0::float, 100::float, random()) as random_seed
	, case 
		when random_seed <= prob then
			true
		else
			false
		end as new_flag
	, e.last_cust_id as customer_id
	, uniform(0::float, 100::float, random()) as random_seed2
	, uniform(0::float, 100::float, random()) as random_seed3
from launched_products as lp
left join dim_products as p 
	on lp.product_id = p.product_id
left join new_logos_curve as n 
	on n.ptype_id = p.ptype_id and months_fix = n.months_since_launch
left join cust_prod_map as m
	on m.product_id = lp.product_id
left join eligible_cust_prod as e
	on e.cust_prod_index = m.next_cust
-- where new_flag = true
), qty_merge as (
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
	where eff_date <= '2025-08-08'
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
	, '2025-08-08' as contract_date
	, n.product_id
	, q.quantity as quantity
	, q.quantity * p.unit_price as contract_amount
	, dateadd('day', do.days_to_start, contract_date)::date as start_date
	, dateadd('year', 1, start_date) as end_date
from __dbt__cte__new_calc as n
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