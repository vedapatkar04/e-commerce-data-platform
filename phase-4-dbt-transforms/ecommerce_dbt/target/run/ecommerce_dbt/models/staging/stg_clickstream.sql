
  create view "ecommerce_db"."dbt_dev_staging"."stg_clickstream__dbt_tmp"
    
    
  as (
    -- Staging model: stg_clickstream
-- File: models/staging/stg_clickstream.sql

WITH source AS (
    SELECT
        event_id::VARCHAR          AS event_id,
        user_id::VARCHAR           AS user_id,
        product_id::VARCHAR        AS product_id,
        event_type,
        session_id::VARCHAR        AS session_id,
        event_at,
        DATE(event_at)             AS event_date,
        EXTRACT(HOUR FROM event_at)AS event_hour
    FROM oltp.clickstream_events
)

SELECT * FROM source
  );