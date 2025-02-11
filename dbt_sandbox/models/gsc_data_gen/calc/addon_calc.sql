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
		o.end_date >= '{{ var("gen_date") }}'
		and o.start_date < '{{ var("gen_date") }}'
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
		and o2.start_date >= '{{ var("gen_date") }}'
	where 
			pending_renewal = false 
		and pending_addon = false
		and e.end_date > '{{ var("gen_date") }}'
)
select
	e.account_id
	, e.product_id
	, e.end_date
	, e.active_quantity
--	, .2 as addon_daily_prob
	, uniform(0::float, 100::float, random()) as random_seed
	, case 
		when random_seed <= {{ var("addon_daily_prob") }} then 
			true 
		else 
			false
		end as addon_flag
	, uniform(0::float, 100::float, random()) as random_seed2
from eligible_to_addon as e
where addon_flag = true