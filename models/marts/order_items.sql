WITH stg_order_items AS (
  SELECT
    *
  FROM {{ ref('stg_order_items') }}
), stg_orders AS (
  SELECT
    *
  FROM {{ ref('stg_orders') }}
), stg_products AS (
  SELECT
    *
  FROM {{ ref('stg_products') }}
), stg_supplies AS (
  SELECT
    PRODUCT_ID,
    SUPPLY_COST
  FROM {{ ref('stg_supplies') }}
), join_1 AS (
  SELECT
    stg_order_items.ORDER_ITEM_ID,
    stg_order_items.ORDER_ID,
    stg_order_items.PRODUCT_ID,
    stg_orders.ORDERED_AT,
    stg_order_items.PRODUCT_ID AS PRODUCT_ID_1
  FROM stg_order_items
  LEFT JOIN stg_orders
    ON stg_order_items.ORDER_ID = stg_orders.ORDER_ID
), aggregate_1 AS (
  SELECT
    PRODUCT_ID,
    SUM(SUPPLY_COST) AS SUPPLY_COST
  FROM stg_supplies
  GROUP BY
    PRODUCT_ID
), join_2 AS (
  SELECT
    join_1.ORDER_ITEM_ID,
    join_1.ORDER_ID,
    join_1.PRODUCT_ID,
    join_1.ORDERED_AT,
    join_1.ORDERED_AT AS ORDERED_AT_1,
    stg_products.PRODUCT_NAME,
    stg_products.PRODUCT_PRICE,
    stg_products.IS_FOOD_ITEM,
    stg_products.IS_DRINK_ITEM,
    join_1.PRODUCT_ID AS PRODUCT_ID_1
  FROM join_1
  LEFT JOIN stg_products
    ON join_1.PRODUCT_ID = stg_products.PRODUCT_ID
), join_3 AS (
  SELECT
    join_2.ORDER_ITEM_ID,
    join_2.ORDER_ID,
    join_2.PRODUCT_ID,
    join_2.ORDERED_AT,
    join_2.PRODUCT_NAME,
    join_2.PRODUCT_PRICE,
    join_2.IS_FOOD_ITEM,
    join_2.IS_DRINK_ITEM,
    join_2.ORDERED_AT AS ORDERED_AT_1,
    join_2.PRODUCT_NAME AS PRODUCT_NAME_1,
    join_2.PRODUCT_PRICE AS PRODUCT_PRICE_1,
    join_2.IS_FOOD_ITEM AS IS_FOOD_ITEM_1,
    join_2.IS_DRINK_ITEM AS IS_DRINK_ITEM_1,
    aggregate_1.SUPPLY_COST
  FROM join_2
  LEFT JOIN aggregate_1
    ON join_2.PRODUCT_ID = aggregate_1.PRODUCT_ID
), order_items AS (
  SELECT
    *
  FROM join_3
)
SELECT
  *
FROM order_items