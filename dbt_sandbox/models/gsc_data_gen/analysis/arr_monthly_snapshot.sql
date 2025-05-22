WITH RECURSIVE month_series (month_date) AS (
    SELECT DATE_TRUNC('month', MIN(start_date)) FROM {{ ref('orders') }}
    UNION ALL
    SELECT DATEADD(month, 1, month_date)
    FROM month_series
    WHERE month_date < (SELECT MAX(end_date) FROM {{ ref('orders') }})
),
all_month_ends AS (
  SELECT DATEADD(day, -1, DATEADD(month, 1, month_date)) as month_end -- Calculate last day of month
  FROM month_series
)
SELECT
    o.account_id,
    o.product_id,
    m.month_end,
    SUM(o.contract_amount / 12) AS monthly_arr_local
FROM
    {{ ref('orders') }} o
JOIN
    all_month_ends m ON o.start_date <= m.month_end AND o.end_date > m.month_end
GROUP BY
    o.account_id,
    o.product_id,
    m.month_end
ORDER BY
    o.account_id,
    o.product_id,
    m.month_end
