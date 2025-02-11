/*
	Append generated orders to existing orders table.
*/

{{ config(
	materialized='incremental'
	)}}

with last_contract as (
	select 
		case when max(contract_id) is null then 10001 else max(contract_id) end as last_contract
	from orders
),
full_orders as (
select
	r.account_id
	, r.contract_date
	, r.contract_amount
	, r.product_id
	, r.qty as quantity 
	, r.start_date 
	, r.end_date 
from {{ ref("renewal_orders") }} as r

union

select
	a.account_id
	, a.contract_date
	, a.contract_amount
	, a.product_id
	, a.addon_quantity as quantity
	, a.start_date 
	, a.end_date 
from {{ ref("addon_orders") }} as a

union

select
	rc.account_id
	, rc.contract_date
	, rc.contract_amount
	, rc.product_id
	, rc.quantity
	, rc.start_date 
	, rc.end_date 
from {{ ref("recapture_orders") }} as rc

union

select
	n.account_id
	, n.contract_date
	, n.contract_amount
	, n.product_id
	, n.quantity
	, n.start_date 
	, n.end_date 
from {{ ref("new_orders") }} as n
)

select
	f.account_id
	, row_number() over (order by uniform(0::float, 100::float, random()) desc) + lc.last_contract as contract_id
	, f.contract_date
	, f.contract_amount
	, f.product_id
	, f.quantity
	, f.start_date
	, f.end_date 
from full_orders as f
join last_contract as lc