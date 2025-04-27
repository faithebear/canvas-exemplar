WITH input_model_1 AS (
  SELECT
    *
  FROM {{ ref('customers') }}
), aggregation_1 AS (
  SELECT
    COUNT(CUSTOMER_ID) AS count_CUSTOMER_ID
  FROM input_model_1
), untitled_sql AS (
  SELECT
    *
  FROM aggregation_1
)
SELECT
  *
FROM untitled_sql