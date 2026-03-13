-- Staging model: stg_products
-- File: models/staging/stg_products.sql

WITH source AS (
    SELECT
        p.product_id::VARCHAR      AS product_id,
        TRIM(p.name)               AS product_name,
        p.price,
        p.stock_qty,
        c.name                     AS category_name,
        p.created_at,

        -- Derived: price band
        CASE
            WHEN p.price < 30   THEN 'budget'
            WHEN p.price < 150  THEN 'mid'
            ELSE                     'premium'
        END                        AS price_band

    FROM oltp.products p
    LEFT JOIN oltp.categories c ON p.category_id = c.category_id
)

SELECT * FROM source