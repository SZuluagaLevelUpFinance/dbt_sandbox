with __dbt__cte__renewals_calc as (
/*
	Establish whether subscriptions eligible to renew will actually renew on the run date given.
*/

with renew_eligible as (
select
	o.account_id 
	, o.product_id 
	, o.end_date
	, sum(o.quantity) as active_quantity
from orders as o
where o.end_date <= dateadd('day', 59, '2025-08-08')
	and o.end_date >= '2025-08-08'
group by o.account_id, o.product_id, o.end_date
)
select 
	r.account_id
	, r.product_id
	, r.end_date
	, r.active_quantity
	, r.end_date - to_date('2025-08-08') + 1 as days_to_expiry
	, p.renewal_prob as renewal_prob_curved
	, case
		when o2.contract_id is null then true 
		else false
		end as eligible_for_renewal
	, case
		when eligible_for_renewal = false
			then 0
		when r.active_quantity <= 10
			then 0.891 * renewal_prob_curved
		when r.active_quantity > 25 
			then 0.932 * renewal_prob_curved
		else
			((r.active_quantity - 9) / 15 * (0.932 - 0.891) + 0.891) * renewal_prob_curved
		end as act_renewal_prob
	, uniform(0::float, 100::float, random()) as random_seed
	, case
		when random_seed <= act_renewal_prob
			then true
		else
			false
		end as renewed_flag
from renew_eligible as r
left join orders as o2
	on o2.account_id = r.account_id
		and o2.product_id = r.product_id
		and o2.start_date = r.end_date
left join input_renewal_curve as p
	on p.days_to_exp = days_to_expiry
where renewed_flag = true
) /*
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
		when random_seed <= 12.5 then
			true
		else 
			false
		end as upsell_at_ren
	, case
		when upsell_at_ren = true then
			false
		when active_quantity = 1 then
			false
		when random_seed >= (100 - 10.0) then
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
from __dbt__cte__renewals_calc as r