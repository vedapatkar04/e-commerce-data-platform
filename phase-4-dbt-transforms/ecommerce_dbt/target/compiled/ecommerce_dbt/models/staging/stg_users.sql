-- Staging model: stg_users
-- File: models/staging/stg_users.sql
--
-- Purpose: Clean and standardize raw users from OLTP
-- Rule: Staging models NEVER join tables — one source per model
-- Materialized as: VIEW (always fresh, no storage cost)

WITH source AS (
    SELECT * FROM oltp.users
),

cleaned AS (
    SELECT
        user_id::VARCHAR           AS user_id,
        TRIM(full_name)            AS full_name,
        TRIM(LOWER(email))         AS email,           -- normalize email
        INITCAP(TRIM(country))     AS country,         -- normalize country casing
        created_at::DATE           AS registered_at,
        is_active,

        -- Derived: days since registration
        CURRENT_DATE - created_at::DATE AS days_since_registered

    FROM source
    WHERE full_name IS NOT NULL    -- basic data quality filter
)

SELECT * FROM cleaned