/*
	Finalize renewal orders in the column format required.
*/

with 
 __dbt__cte__renewals_calc as (
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
),  __dbt__cte__renewals_updown as (
/*
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
), current_price_date as (
	select
		prod_id as product_id
		, max(eff_date) as price_date
	from dim_pricing 
	where eff_date <= '2025-08-08'
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
	, '2025-08-08' as contract_date
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
from __dbt__cte__renewals_updown as r
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