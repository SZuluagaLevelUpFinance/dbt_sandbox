WITH customer_monthly_totals AS (
    SELECT
        account_id,
        month_end,
        SUM(arr_local_eop) AS cust_local_eop,
        SUM(arr_local_bop) AS cust_local_bop
    FROM
        {{ ref('arr_bop_eop_calc') }}
    GROUP BY
        account_id, month_end
)
SELECT
    account_id,
    month_end,
    cust_local_eop,
    cust_local_bop,
    (cust_local_eop - cust_local_bop) AS cust_local_chg,
    CASE
        WHEN cust_local_bop <= 0 AND cust_local_eop > 0 THEN 'New Customer'
        WHEN cust_local_bop > 0 AND cust_local_eop <= 0 THEN 'Lost Customer'
        WHEN cust_local_bop > 0 AND cust_local_eop > 0 AND (cust_local_eop - cust_local_bop) > 0 THEN 'Upsell'
        WHEN cust_local_bop > 0 AND cust_local_eop > 0 AND (cust_local_eop - cust_local_bop) < 0 THEN 'Downsell'
        WHEN (cust_local_eop - cust_local_bop) = 0 THEN 'No Change'
        ELSE 'Undefined' -- This case helps catch unexpected scenarios
    END AS bridge_category
FROM
    customer_monthly_totals
ORDER BY
    account_id, month_end
