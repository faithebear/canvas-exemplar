WITH customers AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'customers') }}
), filter_1 AS (
  SELECT
    *
  FROM customers
  WHERE
    LIFETIME_SPEND_PRETAX > '10'
), my_model_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM my_model_sql