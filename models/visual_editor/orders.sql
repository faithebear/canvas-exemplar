WITH stg_orders AS (
  SELECT
    "order_id" AS ORDER_ID,
    "location_id" AS LOCATION_ID,
    "customer_id" AS CUSTOMER_ID,
    "subtotal_cents" AS SUBTOTAL_CENTS,
    "tax_paid_cents" AS TAX_PAID_CENTS,
    "order_total_cents" AS ORDER_TOTAL_CENTS,
    "subtotal" AS SUBTOTAL,
    "tax_paid" AS TAX_PAID,
    "order_total" AS ORDER_TOTAL,
    "ordered_at" AS ORDERED_AT
  FROM {{ ref('stg_orders') }}
), order_items AS (
  SELECT
    "order_item_id" AS ORDER_ITEM_ID,
    "order_id" AS ORDER_ID,
    "product_id" AS PRODUCT_ID,
    "ordered_at" AS ORDERED_AT,
    "product_name" AS PRODUCT_NAME,
    "product_price" AS PRODUCT_PRICE,
    "is_food_item" AS IS_FOOD_ITEM,
    "is_drink_item" AS IS_DRINK_ITEM,
    "supply_cost" AS SUPPLY_COST
  FROM {{ ref('order_items') }}
), formula_1 AS (
  SELECT
    *,
    CASE WHEN is_food_item THEN 1 ELSE 0 END AS agg__3f7790dc_1a47_47a2_b299_308e9c922cce,
    CASE WHEN is_drink_item THEN 1 ELSE 0 END AS agg__ff3fefa5_510f_4202_b38d_aeeab76f523c
  FROM order_items
), aggregation_1 AS (
  SELECT
    order_id,
    SUM(supply_cost) AS order_cost,
    SUM(product_price) AS order_items_subtotal,
    COUNT(order_item_id) AS count_order_items,
    SUM(agg__3f7790dc_1a47_47a2_b299_308e9c922cce) AS count_food_items,
    SUM(agg__ff3fefa5_510f_4202_b38d_aeeab76f523c) AS count_drink_items,
    MAX(product_price) AS max_product_price
  FROM formula_1
  GROUP BY
    order_id
), join_1 AS (
  SELECT
    *
  FROM stg_orders
  LEFT JOIN aggregation_1
    ON stg_orders.order_id = aggregation_1.order_id
), formula_2 AS (
  SELECT
    *,
    count_food_items > 0 AS is_food_order,
    count_drink_items > 0 AS is_drink_order
  FROM join_1
), formula_3 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at ASC) AS customer_order_number
  FROM formula_2
), orders_sql AS (
  SELECT
    *
  FROM formula_3
)
SELECT
  *
FROM orders_sql