"""
E-Commerce Data Platform — Phase 2

Load layer — writes transformed DataFrames
into the warehouse schema.

Key concept: We use TRUNCATE then INSERT (full refresh)
for dimensions. For the fact table we append only.
This is the simplest load strategy — called
'full refresh'. Phase 4 (dbt) will introduce
incremental loading.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from extract import get_connection


def truncate_table(engine, table: str):
    """
    Clear a table before reloading.
    RESTART IDENTITY resets the SERIAL surrogate keys
    back to 1 on each run — keeps keys consistent.
    CASCADE handles foreign key dependencies.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
    print(f"[LOAD] truncated {table}")


def load_dim_users(df: pd.DataFrame):
    """
    Load users dimension.
    Full refresh — truncate then insert all rows.
    """
    engine = get_connection()
    truncate_table(engine, "warehouse.dim_users")

    df.to_sql(
        name="dim_users",
        schema="warehouse",
        con=engine,
        if_exists="append",   # table already exists, just append
        index=False,          # don't write DataFrame index as a column
        method="multi",       # batch insert for performance
        chunksize=1000,
    )
    print(f"[LOAD] dim_users: {len(df):,} rows loaded")


def load_dim_products(df: pd.DataFrame):
    """Load products dimension."""
    engine = get_connection()
    truncate_table(engine, "warehouse.dim_products")

    df.to_sql(
        name="dim_products",
        schema="warehouse",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"[LOAD] dim_products: {len(df):,} rows loaded")


def load_dim_location(df: pd.DataFrame):
    """Load location dimension."""
    engine = get_connection()
    truncate_table(engine, "warehouse.dim_location")

    df.to_sql(
        name="dim_location",
        schema="warehouse",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"[LOAD] dim_location: {len(df):,} rows loaded")


def load_fact_orders(df: pd.DataFrame):
    """
    Load fact table.
    Also full refresh for simplicity in Phase 2.
    Phase 4 will make this incremental.
    """
    engine = get_connection()
    truncate_table(engine, "warehouse.fact_orders")

    df.to_sql(
        name="fact_orders",
        schema="warehouse",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"[LOAD] fact_orders: {len(df):,} rows loaded")


def validate_load():
    """
    Basic data quality checks after loading.
    Real pipelines use Great Expectations (Phase 5).
    This is our lightweight version.
    """
    engine = get_connection()
    checks = {
        "dim_users":     "SELECT COUNT(*) FROM warehouse.dim_users",
        "dim_products":  "SELECT COUNT(*) FROM warehouse.dim_products",
        "dim_location":  "SELECT COUNT(*) FROM warehouse.dim_location",
        "fact_orders":   "SELECT COUNT(*) FROM warehouse.fact_orders",
    }

    print("\n[VALIDATE] Row counts after load:")
    print("─" * 35)
    with engine.connect() as conn:
        for table, query in checks.items():
            count = conn.execute(text(query)).scalar()
            status = "✅" if count > 0 else "❌"
            print(f"  {status} {table}: {count:,} rows")

    # Check for nulls in fact table keys
    with engine.connect() as conn:
        null_keys = conn.execute(text("""
            SELECT COUNT(*) FROM warehouse.fact_orders
            WHERE user_key IS NULL
            OR product_key IS NULL
            OR date_key IS NULL
        """)).scalar()

    null_status = "✅" if null_keys == 0 else "❌"
    print(f"  {null_status} fact_orders null keys: {null_keys}")
    print("─" * 35)