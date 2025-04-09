WITH input_1 AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'customers') }}
), filter_1 AS (
  SELECT
    *
  FROM input_1
  WHERE
    LIFETIME_SPEND_PRETAX > '10'
), untitled_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM untitled_sql