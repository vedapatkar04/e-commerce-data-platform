"""
E-Commerce Data Platform — Phase 2
 
Extract layer — reads raw data from oltp schema.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text


def get_connection():
    """
    Create SQLAlchemy engine from environment variables.
    In Airflow, these are set in docker-compose.yml.
    Locally, falls back to your Phase 1 credentials.
    """
    host     = os.getenv("ECOMMERCE_DB_HOST", "127.0.0.1")
    port     = os.getenv("ECOMMERCE_DB_PORT", "5433")
    db       = os.getenv("ECOMMERCE_DB_NAME", "ecommerce_db")
    user     = os.getenv("ECOMMERCE_DB_USER", "deuser")
    password = os.getenv("ECOMMERCE_DB_PASSWORD", "depass")

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str)


def extract_users() -> pd.DataFrame:
    """
    Extract all active users from OLTP.
    Returns a DataFrame — think of it as a JS array of objects.
    """
    engine = get_connection()
    query = """
        SELECT
            user_id,
            full_name,
            country,
            created_at::DATE AS registered_at
        FROM oltp.users
        WHERE is_active = TRUE
    """
    df = pd.read_sql(query, engine)
    print(f"[EXTRACT] users: {len(df):,} rows")
    return df


def extract_products() -> pd.DataFrame:
    """
    Extract products joined with category names.
    We flatten the category JOIN here so the warehouse
    dimension table doesn't need to do it later.
    """
    engine = get_connection()
    query = """
        SELECT
            p.product_id,
            p.name,
            p.price,
            c.name AS category
        FROM oltp.products p
        LEFT JOIN oltp.categories c ON p.category_id = c.category_id
    """
    df = pd.read_sql(query, engine)
    print(f"[EXTRACT] products: {len(df):,} rows")
    return df


def extract_orders() -> pd.DataFrame:
    """
    Extract delivered orders joined with order items.
    We only load DELIVERED orders into the warehouse
    — pending/cancelled orders are not yet revenue.

    This is called the 'grain' decision — what level
    of detail does the fact table represent?
    Answer: one row per order line item.
    """
    engine = get_connection()
    query = """
        SELECT
            o.order_id,
            o.user_id,
            o.status          AS order_status,
            o.created_at,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.discount
        FROM oltp.orders o
        JOIN oltp.order_items oi ON o.order_id = oi.order_id
        WHERE o.status = 'delivered'
    """
    df = pd.read_sql(query, engine)
    print(f"[EXTRACT] orders + items: {len(df):,} rows")
    return df


def extract_locations() -> pd.DataFrame:
    """
    Extract unique countries from users.
    We derive a location dimension from user country data.
    """
    engine = get_connection()
    query = """
        SELECT DISTINCT country
        FROM oltp.users
        WHERE country IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    print(f"[EXTRACT] locations: {len(df):,} rows")
    return df