/*
    Model: append/orders.sql
    Purpose: Consolidates various types of order data (new, renewal, addon, recapture) 
             from the 'prep' stage models into a single incremental table. It also
             generates unique contract_ids.

    Important Note on `contract_amount` and `dim_pricing`:
    - Previously, this model had a fallback mechanism to calculate `contract_amount` using
      `quantity * unit_price` via a join to `dim_pricing`.
    - This join (`ON f.product_id = dp.prod_id`) was problematic because `dim_pricing`
      contains multiple pricing tiers (based on quantity and effective date) for each product.
      The simple join caused a fan-out (row duplication) and could lead to incorrect
      `unit_price` selection if the fallback was triggered.
    - Investigation confirmed that all upstream 'prep' models (`new_orders`, `renewal_orders`,
      `addon_orders`, `recapture_orders`) correctly calculate and provide the `contract_amount`,
      including selecting the appropriate price tier from `dim_pricing` based on quantity
      and contract date (or effective term for add-ons).
    - Therefore, the join to `dim_pricing` and the fallback calculation for `contract_amount`
      have been removed from this model (as of <current date or relevant commit reference if known by worker>).
    - This model now relies on `f.contract_amount` being accurately provided by the upstream CTEs 
      in `full_orders`. This resolves potential row duplication and ensures `contract_amount` 
      accuracy based on the specific logic in each 'prep' model.
*/

{{ config(
    materialized='incremental'
) }}

with last_contract as (
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
    from {{ ref("renewal_orders") }} as r

    union

    select
        a.account_id,
        a.contract_date,
        a.contract_amount,
        a.product_id,
        a.addon_quantity as quantity,
        a.start_date,
        a.end_date 
    from {{ ref("addon_orders") }} as a

    union

    select
        rc.account_id,
        rc.contract_date,
        rc.contract_amount,
        rc.product_id,
        rc.quantity,
        rc.start_date,
        rc.end_date 
    from {{ ref("recapture_orders") }} as rc

    union

    select
        n.account_id,
        n.contract_date,
        n.contract_amount,
        n.product_id,
        n.quantity,
        n.start_date,
        n.end_date 
    from {{ ref("new_orders") }} as n
)

select
    f.account_id,
    row_number() over (order by uniform(0::float, 100::float, random()) desc) + lc.last_contract as contract_id,
    f.contract_date,
    f.contract_amount,
    f.product_id,
    f.quantity,
    f.start_date,
    f.end_date 
from full_orders as f
join last_contract as lc on 1=1
