/*
	Figure out which products will have a new customer added.
*/

with launched_products as (
	select 
		p.product_id
		, datediff('month', p.intro_date, '{{ var("gen_date") }}') as months_since_launch
	from dim_products as p
	where p.intro_date <= '{{ var("gen_date") }}'
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
	, n.new_logos_per_month / date_part('day', last_day('{{ var("gen_date") }}'::date)) * 100 as prob
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