"""
E-Commerce Data Platform — Phase 2
"""

from datetime import datetime, timedelta
import sys
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/etl")


# ─────────────────────────────────────────────────────
# Helpers — clean and serialize DataFrames for XCom
# Strip non-ASCII characters to avoid UTF-8 issues
# ─────────────────────────────────────────────────────
def df_to_xcom(df):
    """
    Clean string columns and serialize to JSON for XCom.
    Strips non-ASCII characters that break PostgreSQL storage.
    """
    df = df.copy()
    # Clean all string columns — remove non-ASCII characters
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.encode("ascii", errors="ignore").str.decode("ascii")
    # Convert timestamps to string
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].astype(str)
    return df.to_json(orient="records")


def xcom_to_df(data):
    """Deserialize JSON string back to DataFrame."""
    import pandas as pd
    return pd.read_json(data, orient="records")


# ─────────────────────────────────────────────────────
# Default args
# ─────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "depends_on_past": False,
}


# ─────────────────────────────────────────────────────
# Task functions
# ─────────────────────────────────────────────────────
def task_extract_and_transform_users(**context):
    from extract import extract_users
    from transform import transform_users
    raw = extract_users()
    transformed = transform_users(raw)
    context["ti"].xcom_push(key="dim_users", value=df_to_xcom(transformed))


def task_extract_and_transform_products(**context):
    from extract import extract_products
    from transform import transform_products
    raw = extract_products()
    transformed = transform_products(raw)
    context["ti"].xcom_push(key="dim_products", value=df_to_xcom(transformed))


def task_extract_and_transform_locations(**context):
    from extract import extract_locations
    from transform import transform_locations
    raw = extract_locations()
    transformed = transform_locations(raw)
    context["ti"].xcom_push(key="dim_location", value=df_to_xcom(transformed))


def task_extract_orders(**context):
    from extract import extract_orders
    raw = extract_orders()
    context["ti"].xcom_push(key="raw_orders", value=df_to_xcom(raw))


def task_load_dim_users(**context):
    from load import load_dim_users
    data = context["ti"].xcom_pull(key="dim_users", task_ids="extract_transform_users")
    load_dim_users(xcom_to_df(data))


def task_load_dim_products(**context):
    from load import load_dim_products
    data = context["ti"].xcom_pull(key="dim_products", task_ids="extract_transform_products")
    load_dim_products(xcom_to_df(data))


def task_load_dim_location(**context):
    from load import load_dim_location
    data = context["ti"].xcom_pull(key="dim_location", task_ids="extract_transform_locations")
    load_dim_location(xcom_to_df(data))


def task_transform_and_load_facts(**context):
    from transform import transform_orders
    from load import load_fact_orders
    ti = context["ti"]

    raw_orders   = xcom_to_df(ti.xcom_pull(key="raw_orders",   task_ids="extract_orders"))
    dim_users    = xcom_to_df(ti.xcom_pull(key="dim_users",    task_ids="extract_transform_users"))
    dim_products = xcom_to_df(ti.xcom_pull(key="dim_products", task_ids="extract_transform_products"))
    dim_location = xcom_to_df(ti.xcom_pull(key="dim_location", task_ids="extract_transform_locations"))

    fact_df = transform_orders(raw_orders, dim_users, dim_products, dim_location)
    load_fact_orders(fact_df)


def task_validate(**context):
    from load import validate_load
    validate_load()


# ─────────────────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_etl",
    description="Daily ETL: OLTP → Warehouse star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ecommerce", "etl", "phase-2"],
) as dag:

    t_users = PythonOperator(
        task_id="extract_transform_users",
        python_callable=task_extract_and_transform_users,
    )

    t_products = PythonOperator(
        task_id="extract_transform_products",
        python_callable=task_extract_and_transform_products,
    )

    t_locations = PythonOperator(
        task_id="extract_transform_locations",
        python_callable=task_extract_and_transform_locations,
    )

    t_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=task_extract_orders,
    )

    t_load_users = PythonOperator(
        task_id="load_dim_users",
        python_callable=task_load_dim_users,
    )

    t_load_products = PythonOperator(
        task_id="load_dim_products",
        python_callable=task_load_dim_products,
    )

    t_load_location = PythonOperator(
        task_id="load_dim_location",
        python_callable=task_load_dim_location,
    )

    t_load_facts = PythonOperator(
        task_id="load_fact_orders",
        python_callable=task_transform_and_load_facts,
    )

    t_validate = PythonOperator(
        task_id="validate_load",
        python_callable=task_validate,
    )

    # Dependencies
    t_users     >> t_load_users
    t_products  >> t_load_products
    t_locations >> t_load_location

    [t_load_users, t_load_products, t_load_location, t_orders] >> t_load_facts

    t_load_facts >> t_validate