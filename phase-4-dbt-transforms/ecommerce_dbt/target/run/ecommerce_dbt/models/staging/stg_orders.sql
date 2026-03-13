
  create view "ecommerce_db"."dbt_dev_staging"."stg_orders__dbt_tmp"
    
    
  as (
    -- Staging model: stg_orders
-- File: models/staging/stg_orders.sql
--
-- Purpose: Clean raw orders, join with order_items
-- Grain: one row per order line item

WITH orders AS (
    SELECT * FROM oltp.orders
),

order_items AS (
    SELECT * FROM oltp.order_items
),

joined AS (
    SELECT
        o.order_id::VARCHAR        AS order_id,
        o.user_id::VARCHAR         AS user_id,
        o.status                   AS order_status,
        o.created_at               AS ordered_at,
        o.shipped_at,
        oi.product_id::VARCHAR     AS product_id,
        oi.quantity,
        oi.unit_price,
        oi.discount,

        -- Calculate line item revenue
        ROUND(
            (oi.unit_price * oi.quantity) * (1 - oi.discount / 100), 2
        )                          AS line_revenue,

        -- Derived: days to ship
        CASE
            WHEN o.shipped_at IS NOT NULL
            THEN (o.shipped_at::DATE - o.created_at::DATE)
            ELSE NULL
        END                        AS days_to_ship

    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
)

SELECT * FROM joined
  );