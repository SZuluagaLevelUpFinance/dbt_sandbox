{{ config(materialized='table') }}

select
	current_timestamp() as last_run_datetime
	, current_date() as last_run_date
	, '{{ var("gen_date") }}'::date as last_gen_date