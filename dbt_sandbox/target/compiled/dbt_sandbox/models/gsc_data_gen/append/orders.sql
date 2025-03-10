/*
    Append generated orders to existing orders table.
*/



with  __dbt__cte__renewals_calc as (
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
),  __dbt__cte__renewal_orders as (
/*
	Finalize renewal orders in the column format required.
*/

with 
current_price_date as (
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
),  __dbt__cte__addon_calc as (
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
),  __dbt__cte__addon_orders as (
/* 
	Calculate actual addon quantity and prepare for union.
*/

with qty_merge as (
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
),  __dbt__cte__recapture_calc as (
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
),  __dbt__cte__recapture_orders as (
with qty_merge as (
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
),  __dbt__cte__new_calc as (
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
),  __dbt__cte__new_orders as (
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
), last_contract as (
    select 
        case when max(contract_id) is null then 10001 else max(contract_id) end as last_contract
    from orders
),
full_orders as (
    select
        r.account_id,
        r.contract_date,
        r.contract_amount,
        r.product_id,
        r.qty as quantity,
        r.start_date,
        r.end_date 
    from __dbt__cte__renewal_orders as r

    union

    select
        a.account_id,
        a.contract_date,
        a.contract_amount,
        a.product_id,
        a.addon_quantity as quantity,
        a.start_date,
        a.end_date 
    from __dbt__cte__addon_orders as a

    union

    select
        rc.account_id,
        rc.contract_date,
        rc.contract_amount,
        rc.product_id,
        rc.quantity,
        rc.start_date,
        rc.end_date 
    from __dbt__cte__recapture_orders as rc

    union

    select
        n.account_id,
        n.contract_date,
        n.contract_amount,
        n.product_id,
        n.quantity,
        n.start_date,
        n.end_date 
    from __dbt__cte__new_orders as n
)

select
    f.account_id,
    row_number() over (order by uniform(0::float, 100::float, random()) desc) + lc.last_contract as contract_id,
    f.contract_date,
    coalesce(f.contract_amount, f.quantity * dp.unit_price) as contract_amount,
    f.product_id,
    f.quantity,
    f.start_date,
    f.end_date 
from full_orders as f
join last_contract as lc on 1=1
join dim_pricing as dp
    on f.product_id = dp.prod_id