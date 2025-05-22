WITH current_month_arr AS (
    SELECT
        account_id,
        product_id,
        month_end,
        monthly_arr_local AS arr_local_eop,
        LAG(monthly_arr_local, 1, 0) OVER (PARTITION BY account_id, product_id ORDER BY month_end) AS arr_local_bop
    FROM
        {{ ref('arr_monthly_snapshot') }}
)
SELECT
    account_id,
    product_id,
    month_end,
    arr_local_eop,
    arr_local_bop,
    (arr_local_eop - arr_local_bop) AS arr_local_chg
FROM
    current_month_arr
ORDER BY
    account_id, product_id, month_end
