
  
    

  create  table "ecommerce_db"."dbt_dev_marts"."mart_revenue__dbt_tmp"
  
  
    as
  
  (
    -- Mart model: mart_revenue
-- File: models/marts/mart_revenue.sql
--
-- Purpose: Daily and monthly revenue summary
-- This is what a BI dashboard or analyst queries directly
-- Materialized as TABLE — fast reads, pre-aggregated

WITH enriched AS (
    SELECT * FROM "ecommerce_db"."dbt_dev_intermediate"."int_order_items_enriched"
),

-- Only count delivered orders as revenue
delivered AS (
    SELECT * FROM enriched
    WHERE order_status = 'delivered'
),

daily_revenue AS (
    SELECT
        order_date,
        order_year,
        order_month,
        order_month_name,
        country,
        category_name,
        price_band,

        -- Metrics
        COUNT(DISTINCT order_id)    AS total_orders,
        COUNT(*)                    AS total_line_items,
        SUM(quantity)               AS total_units_sold,
        ROUND(SUM(line_revenue)::NUMERIC, 2)   AS total_revenue,
        ROUND(AVG(line_revenue)::NUMERIC, 2)   AS avg_order_value,
        ROUND(SUM(discount)::NUMERIC, 2)       AS total_discount_pct

    FROM delivered
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM daily_revenue
ORDER BY order_date DESC
  );
  