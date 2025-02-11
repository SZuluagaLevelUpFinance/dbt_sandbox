/*
	Provides lookup for current active pricing by product.
*/

select
	prod_id as product_id
	, max(eff_date) as price_date
from dim_pricing 
where eff_date <= '{{ var("gen_date") }}'
group by product_id