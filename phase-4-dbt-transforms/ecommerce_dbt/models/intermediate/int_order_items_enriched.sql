-- Intermediate model: int_order_items_enriched
-- File: models/intermediate/int_order_items_enriched.sql
--
-- Purpose: Join orders with user and product context
-- This is where we combine staging models into one enriched dataset
-- Mart models will query THIS instead of joining everything themselves
--
-- Key concept: ref() function
-- ref('stg_orders') tells dbt to depend on stg_orders model
-- dbt builds stg_orders BEFORE this model automatically

WITH orders AS (
    -- ref() creates dependency — dbt runs stg_orders first
    SELECT * FROM {{ ref('stg_orders') }}
),

users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT
        -- Order details
        o.order_id,
        o.order_status,
        o.ordered_at,
        o.days_to_ship,
        o.quantity,
        o.unit_price,
        o.discount,
        o.line_revenue,

        -- User context (denormalized in)
        u.user_id,
        u.full_name         AS customer_name,
        u.country,
        u.registered_at,
        u.days_since_registered,

        -- Product context (denormalized in)
        p.product_id,
        p.product_name,
        p.category_name,
        p.price_band,

        -- Date dimensions for grouping
        DATE(o.ordered_at)                    AS order_date,
        EXTRACT(YEAR  FROM o.ordered_at)::INT AS order_year,
        EXTRACT(MONTH FROM o.ordered_at)::INT AS order_month,
        TO_CHAR(o.ordered_at, 'Month')        AS order_month_name,
        EXTRACT(DOW   FROM o.ordered_at)::INT AS order_day_of_week

    FROM orders o
    LEFT JOIN users    u ON o.user_id    = u.user_id
    LEFT JOIN products p ON o.product_id = p.product_id
)

SELECT * FROM enriched