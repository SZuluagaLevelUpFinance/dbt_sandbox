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