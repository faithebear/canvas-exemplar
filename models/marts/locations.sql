WITH stg_locations AS (
  SELECT
    *
  FROM {{ ref('stg_locations') }}
), locations AS (
  SELECT
    *
  FROM stg_locations
)
SELECT
  *
FROM locations