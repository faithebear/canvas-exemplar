WITH input_model_1 AS (
  SELECT
    *
  FROM {{ ref('orders') }}
), order_1 AS (
  SELECT
    *
  FROM input_model_1
  ORDER BY
    SUBTOTAL_CENTS ASC
), tylers_annual_10k_sql AS (
  SELECT
    *
  FROM order_1
)
SELECT
  *
FROM tylers_annual_10k_sql