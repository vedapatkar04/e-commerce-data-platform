
  
    

  create  table "ecommerce_db"."dbt_dev_marts"."mart_top_products__dbt_tmp"
  
  
    as
  
  (
    -- Mart model: mart_top_products
-- File: models/marts/mart_top_products.sql
--
-- Purpose: Product performance analytics
-- Answers: what are our best and worst performing products?

WITH enriched AS (
    SELECT * FROM "ecommerce_db"."dbt_dev_intermediate"."int_order_items_enriched"
    WHERE order_status = 'delivered'
),

product_stats AS (
    SELECT
        product_id,
        product_name,
        category_name,
        price_band,

        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(quantity)                       AS total_units_sold,
        ROUND(SUM(line_revenue)::NUMERIC, 2)AS total_revenue,
        ROUND(AVG(unit_price)::NUMERIC, 2)  AS avg_selling_price,
        ROUND(AVG(discount)::NUMERIC, 2)    AS avg_discount_pct,

        -- Rank by revenue within category
        RANK() OVER (
            PARTITION BY category_name
            ORDER BY SUM(line_revenue) DESC
        )                                   AS revenue_rank_in_category

    FROM enriched
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM product_stats
ORDER BY total_revenue DESC
  );
  