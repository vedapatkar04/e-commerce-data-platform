-- =============================================================
-- E-Commerce Data Platform
-- Phase 1 — Schema Initialization
-- =============================================================

-- =============================================================
-- SCHEMA 1: OLTP (Transactional / Source)
-- Normalized, write-optimized
-- =============================================================

-- Create a schema named oltp
CREATE SCHEMA IF NOT EXISTS oltp;

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users
CREATE TABLE IF NOT EXISTS oltp.users (
    user_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Categories
CREATE TABLE IF NOT EXISTS oltp.categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    parent_category_id INT REFERENCES oltp.categories(category_id) ON DELETE SET NULL 
);

-- Products
CREATE TABLE IF NOT EXISTS oltp.products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category_id INT REFERENCES oltp.categories(category_id) ON DELETE SET NULL,
    name VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >=0),
    stock_qty INT NOT NULL DEFAULT 0 CHECK (stock_qty >=0),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Orders
-- Create enum
CREATE TYPE oltp.order_status AS ENUM (
    'pending', 'processing', 'shipped', 'delivered', 'cancelled'
);

CREATE TABLE IF NOT EXISTS oltp.orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES oltp.users(user_id) uuid_generate_v4(),
    status oltp.order_status NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(10, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    shipped_at TIMESTAMP
);

-- Order items One order can have many product 
CREATE TABLE IF NOT EXISTS oltp.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES oltp.orders(order_id) ON DELETE CASCADE,
    product_id UUID NOT NULL REFERENCES oltp.products(product_id) ON DELETE RESTRICT,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL,
    discount NUMERIC(10, 2) DEFAULT 0 CHECK (discount >= 0 AND delivered <= 100)
);

-- Click stream event forr tracking user behaviour
CREATE TYPE oltp.event_type AS ENUM (
    'page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase'
);
CREATE TABLE IF NOT EXISTS oltp.clickstream_events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES oltp.users(user_id) ON DELETE SET NULL,
    product_id UUID REFERENCES oltp.products(product_id) ON DELETE SET NULL,
    event_type oltp.event_type NOT NULL,
    session_id UUID NOT NULL,
    event_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Index
CREATE INDEX idx_orders_user_id ON oltp.orders(order_id);
CREATE INDEX idx_orders_created_at ON oltp.orders(created_at);
CREATE INDEX idx_order_items_order ON oltp.order_items(order_id);
CREATE INDEX idx_clickstream_user ON oltp.clickstream_events(user_id);
CREATE INDEX idx_clickstream_event ON oltp.clickstream_events(event_at);

-- =============================================================
-- SCHEMA 2: OLAP (Analytical / Data Warehouse)
-- Star schema — denormalized, read-optimized
-- =============================================================

-- Create a schema named  warehouse
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Dimension
-- dim_date
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    day_num INT,
    month_num INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);
-- Pre populate date for 2022 2026
INSERT INTO warehouse.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::DATE AS full_date,
    TO_CHAR(d, 'Day') AS day_of_week,
    EXTRACT(DAY FROM d)::INT AS day_num,
    EXTRACT(MONTH FROM d)::INT AS month_num,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend
FROM generate_series('2022-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- dim_users
CREATE TABLE IF NOT EXISTS warehouse.dim_users (
    user_key SERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    full_name VARCHAR(255),
    country VARCHAR(100),
    segment VARCHAR(50),
    registered_at DATE
);

-- dim_products
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id UUID NOT NULL,
    name VARCHAR(255),
    category VARCHAR(100),
    price_band VARCHAR(20)
);

-- dim_loc
CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_key SERIAL PRIMARY KEY,
    country VARCHAR(100),
    region VARCHAR(100)
);

-- Fact
-- fact_orders
CREATE TABLE IF NOT EXISTS warehouse.fact_orders (
    order_item_key  SERIAL PRIMARY KEY,
    user_key INT REFERENCES warehouse.dim_users(user_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    date_key INT REFERENCES warehouse.dim_date(date_key),
    location_key INT REFERENCES warehouse.dim_location(location_key),
    -- Source
    order_id        UUID,
    -- Measures
    quantity INT,
    unit_price NUMERIC(10, 2),
    discount_amt NUMERIC(10, 2),
    total_revenue NUMERIC(10, 2),
    order_status VARCHAR(20)
);

-- =============================================================
-- Run: docker-compose up -d
-- Connect on: localhost:5432
-- DB: ecommerce_db | User: admin | Password: admin123
-- pgAdmin UI: http://localhost:5050
-- =============================================================