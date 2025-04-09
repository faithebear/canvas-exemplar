WITH raw_orders AS (
  SELECT
    *
  FROM {{ source('ecom', 'raw_orders') }}
), formula_1 AS (
  SELECT
    ORDER_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    SUBTOTAL_CENTS,
    TAX_PAID_CENTS,
    ORDER_TOTAL_CENTS,
    CAST((
      SUBTOTAL_CENTS / 100
    ) AS DECIMAL(16, 2)) AS SUBTOTAL,
    CAST((
      TAX_PAID_CENTS / 100
    ) AS DECIMAL(16, 2)) AS TAX_PAID,
    CAST((
      ORDER_TOTAL_CENTS / 100
    ) AS DECIMAL(16, 2)) AS ORDER_TOTAL,
    DATE_TRUNC('DAY', ORDERED_AT) AS ORDERED_AT,
    CASE WHEN (
      TAX_PAID_CENTS / 100
    ) > 2 THEN 'high tax' ELSE 'normal tax' END AS TAX_CATEGORY
  FROM raw_orders
), filter_1 AS (
  SELECT
    *
  FROM formula_1
  WHERE
    TAX_PAID > '10'
), generated AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM generated