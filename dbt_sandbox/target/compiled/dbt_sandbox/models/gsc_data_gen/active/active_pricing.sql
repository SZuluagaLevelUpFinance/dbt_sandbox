/*
	Provides lookup for current active pricing by product.
*/

select
	prod_id as product_id
	, max(eff_date) as price_date
from dim_pricing 
where eff_date <= '2025-08-08'
group by product_id