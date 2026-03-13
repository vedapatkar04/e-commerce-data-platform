-- Mart model: mart_funnel
-- File: models/marts/mart_funnel.sql
--
-- Purpose: Clickstream funnel analysis
-- Shows drop-off at each stage of the user journey
-- Key business question: where are users abandoning?

WITH clickstream AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
),

daily_funnel AS (
    SELECT
        event_date,
        event_type,
        COUNT(*)                        AS event_count,
        COUNT(DISTINCT user_id)         AS unique_users,
        COUNT(DISTINCT session_id)      AS unique_sessions
    FROM clickstream
    GROUP BY 1, 2
),

-- Pivot funnel stages into columns for easy analysis
funnel_summary AS (
    SELECT
        event_date,
        SUM(CASE WHEN event_type = 'page_view'        THEN unique_users ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'product_view'     THEN unique_users ELSE 0 END) AS product_views,
        SUM(CASE WHEN event_type = 'add_to_cart'      THEN unique_users ELSE 0 END) AS add_to_carts,
        SUM(CASE WHEN event_type = 'checkout'         THEN unique_users ELSE 0 END) AS checkouts,
        SUM(CASE WHEN event_type = 'purchase'         THEN unique_users ELSE 0 END) AS purchases,

        -- Conversion rates between stages
        ROUND(
            100.0 * SUM(CASE WHEN event_type = 'product_view' THEN unique_users ELSE 0 END)
            / NULLIF(SUM(CASE WHEN event_type = 'page_view' THEN unique_users ELSE 0 END), 0)
        , 2)                           AS page_to_product_pct,

        ROUND(
            100.0 * SUM(CASE WHEN event_type = 'add_to_cart' THEN unique_users ELSE 0 END)
            / NULLIF(SUM(CASE WHEN event_type = 'product_view' THEN unique_users ELSE 0 END), 0)
        , 2)                           AS product_to_cart_pct,

        ROUND(
            100.0 * SUM(CASE WHEN event_type = 'purchase' THEN unique_users ELSE 0 END)
            / NULLIF(SUM(CASE WHEN event_type = 'add_to_cart' THEN unique_users ELSE 0 END), 0)
        , 2)                           AS cart_to_purchase_pct

    FROM daily_funnel
    GROUP BY event_date
)

SELECT * FROM funnel_summary
ORDER BY event_date DESC