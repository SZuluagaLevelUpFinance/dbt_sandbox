# Introduction:
This dbt project generates order data for Generic SaaS Company (GSC), a fictional company that sells software subscriptions.
It is intended to create a clean and idealized data set for use in training and for client demonstrations. Multiple use-cases
will be covered by this data... eventually.

Last updated: 6/21/2023

# To Do List:
* Create renewal logic and generate renewal orders. **done**
* Create addon logic and generate addon orders. **done**
* Create recapture logic and generate recapture orders. **done**
* Create new and cross-sell logic and generate orders. **done** 
* Adjust contract_id logic to generate from consolidated list of all generated orders in a random order. **done**

* Add product type based probabilities for addons **done**

* Create python script to run dbt script through every day from 11/2/2021 through today. **done**
* Add query language to find last generation date and only run from then. **done**

* Fix references to seed files instead of pre-seeded tables.

# Project variables:

# Seeds:

## Dimensional inputs:
Mapping and reference tables for dimensions on customer, product, order, etc.

* dim_channel
	* Reference table for different order channels.
	* Not currently used.
* dim_cust_type
	* Reference table for customer types.
	* Mapped to customer list.
	* Not currently used.
* dim_pfamily
	* Reference table for product families.
	* Maps to dim_products
* dim_pricing
	* Pricing table for different products.
	* Currently only has prices for 1 product.
* dim_products
	* Master list of products (current and future).
* dim_ptype
	* Categorizes products by type.

## Probability curves:
To support pseudo-random generation of order data, these tables contain weighted curves to adjust probabilities of specific
events. These can be changed and re-seeded to alter future behavior, but will not affect past events.

* input_renewal_curve
	* Time-series renewal probability based on number of days until the existing subscription expires.
	* The vast majority of customers are expected to renew at the last minute.
	* The probabilities must add up to 1.00 representing 100% total probability.
* up_at_ren_prob_curve and down_at_ren_prob_curve
	* Baseline probability for the quantity up or downsold, if the renewal was calculated to up or downsell during renewal.
	* Each table must have individual quantity probabilities add to 100.
* new_qty_prob_curve
	* For new customers, the probability distribution of the initial quantity.