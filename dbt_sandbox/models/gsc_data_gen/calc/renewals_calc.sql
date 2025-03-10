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
where o.end_date <= dateadd('day', 59, '{{ var("gen_date") }}')
	and o.end_date >= '{{ var("gen_date") }}'
group by o.account_id, o.product_id, o.end_date
)
select 
	r.account_id
	, r.product_id
	, r.end_date
	, r.active_quantity
	, r.end_date - to_date('{{ var("gen_date") }}') + 1 as days_to_expiry
	, p.renewal_prob as renewal_prob_curved
	, case
		when o2.contract_id is null then true 
		else false
		end as eligible_for_renewal
	, case
		when eligible_for_renewal = false
			then 0
		when r.active_quantity <= 10
			then {{ var("base_renewal_prob") }} * renewal_prob_curved
		when r.active_quantity > 25 
			then {{ var("max_renewal_prob") }} * renewal_prob_curved
		else
			((r.active_quantity - 9) / 15 * ({{ var("max_renewal_prob") }} - {{ var("base_renewal_prob") }}) + {{ var("base_renewal_prob") }}) * renewal_prob_curved
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
left join {{ ref("input_renewal_curve") }} as p
  on p.days_to_exp = days_to_expiry
where renewed_flag = true