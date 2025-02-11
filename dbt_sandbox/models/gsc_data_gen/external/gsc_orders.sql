/*
	Copy orders for Shiyao's schema
*/

{{ config(
	materialized="table",
	schema="dbt_exercise_sl",
	enabled=false
)}}

with dummy as (
select *
from {{ ref("orders") }}
)

select
	*
from orders