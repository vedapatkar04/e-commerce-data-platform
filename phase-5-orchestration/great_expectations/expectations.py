"""
E-Commerce Data Platform — Phase 5

Data Quality checks using pandas + psycopg2.
Same concept as Great Expectations but zero extra dependencies.

Run:
    python expectations.py
"""

import os
import sys
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

# ─────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("ECOMMERCE_DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("ECOMMERCE_DB_PORT", "5433")),
    "dbname":   os.getenv("ECOMMERCE_DB_NAME", "ecommerce_db"),
    "user":     os.getenv("ECOMMERCE_DB_USER", "deuser"),
    "password": os.getenv("ECOMMERCE_DB_PASSWORD", "depass"),
}

results = []


def get_engine():
    c = DB_CONFIG
    return create_engine(
        f"postgresql+psycopg2://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['dbname']}"
    )


def load(query: str) -> pd.DataFrame:
    return pd.read_sql(query, get_engine())


# ─────────────────────────────────────────────────────
# Expectation helpers
# Mirror Great Expectations API but using pandas
# ─────────────────────────────────────────────────────
def expect_not_null(df: pd.DataFrame, col: str, label: str):
    nulls  = df[col].isnull().sum()
    passed = nulls == 0
    _record(f"{label} — {col} not null", passed,
            f"{nulls} nulls found" if not passed else "")


def expect_unique(df: pd.DataFrame, col: str, label: str):
    dupes  = df[col].duplicated().sum()
    passed = dupes == 0
    _record(f"{label} — {col} unique", passed,
            f"{dupes} duplicates found" if not passed else "")


def expect_min_rows(df: pd.DataFrame, min_rows: int, label: str):
    count  = len(df)
    passed = count >= min_rows
    _record(f"{label} — row count >= {min_rows:,}", passed,
            f"got {count:,}" if not passed else f"{count:,} rows")


def expect_accepted_values(df: pd.DataFrame, col: str,
                           valid: list, label: str):
    invalid = df[~df[col].isin(valid)][col].unique()
    passed  = len(invalid) == 0
    _record(f"{label} — {col} accepted values", passed,
            f"invalid: {list(invalid)}" if not passed else "")


def expect_non_negative(df: pd.DataFrame, col: str, label: str):
    negs   = (df[col] < 0).sum()
    passed = negs == 0
    _record(f"{label} — {col} >= 0", passed,
            f"{negs} negative values" if not passed else "")


def _record(name: str, passed: bool, detail: str = ""):
    status = "✅ PASS" if passed else "❌ FAIL"
    results.append({"name": name, "passed": passed})
    detail_str = f"  ({detail})" if detail else ""
    print(f"  {status}  {name}{detail_str}")


# ─────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────
def check_users():
    print("\n📋 OLTP — users")
    df = load("SELECT * FROM oltp.users")
    expect_not_null(df, "user_id", "users")
    expect_not_null(df, "email", "users")
    expect_not_null(df, "country", "users")
    expect_unique(df, "user_id", "users")
    expect_unique(df, "email", "users")
    expect_min_rows(df, 1000, "users")


def check_orders():
    print("\n📋 OLTP — orders")
    df = load("SELECT * FROM oltp.orders")
    expect_not_null(df, "order_id", "orders")
    expect_not_null(df, "user_id", "orders")
    expect_not_null(df, "status", "orders")
    expect_accepted_values(df, "status",
        ["pending", "processing", "shipped", "delivered", "cancelled"],
        "orders")
    expect_non_negative(df, "total_amount", "orders")
    expect_min_rows(df, 10000, "orders")


def check_products():
    print("\n📋 OLTP — products")
    df = load("SELECT * FROM oltp.products")
    expect_not_null(df, "product_id", "products")
    expect_unique(df, "product_id", "products")
    expect_non_negative(df, "price", "products")
    expect_non_negative(df, "stock_qty", "products")
    expect_min_rows(df, 500, "products")


def check_clickstream():
    print("\n📋 OLTP — clickstream_events")
    df = load("SELECT * FROM oltp.clickstream_events LIMIT 10000")
    expect_not_null(df, "event_id", "clickstream")
    expect_not_null(df, "event_at", "clickstream")
    expect_unique(df, "event_id", "clickstream")
    expect_accepted_values(df, "event_type",
        ["page_view", "product_view", "add_to_cart",
         "remove_from_cart", "checkout", "purchase"],
        "clickstream")
    expect_min_rows(df, 1000, "clickstream sample")


def check_warehouse():
    print("\n🏭 Warehouse — fact_orders")
    df = load("SELECT * FROM warehouse.fact_orders")
    expect_not_null(df, "user_key", "fact_orders")
    expect_not_null(df, "product_key", "fact_orders")
    expect_not_null(df, "date_key", "fact_orders")
    expect_non_negative(df, "total_revenue", "fact_orders")
    expect_min_rows(df, 1, "fact_orders")

    print("\n🏭 Warehouse — dimensions")
    for table, min_rows in [
        ("dim_users", 100), ("dim_products", 100), ("dim_location", 1)
    ]:
        df = load(f"SELECT * FROM warehouse.{table}")
        expect_min_rows(df, min_rows, table)


def check_dbt_marts():
    print("\n🔷 dbt Marts")
    engine = get_engine()
    for mart in ["mart_revenue", "mart_funnel", "mart_top_products"]:
        try:
            df = pd.read_sql(
                f"SELECT COUNT(*) AS cnt FROM dbt_dev_marts.{mart}",
                engine
            )
            count  = df["cnt"].iloc[0]
            passed = count > 0
            results.append({"name": f"{mart} has data", "passed": passed})
            status = "✅ PASS" if passed else "❌ FAIL"
            print(f"  {status}  {mart} has data ({count:,} rows)")
        except Exception as e:
            results.append({"name": f"{mart} exists", "passed": False})
            print(f"  ❌ FAIL  {mart} — {e}")


# ─────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────
def print_summary():
    total  = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed
    pct    = round(100 * passed / total) if total > 0 else 0

    print("\n" + "=" * 55)
    print(f"  DATA QUALITY REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
    print(f"  Total  : {total}")
    print(f"  ✅ Pass : {passed}")
    print(f"  ❌ Fail : {failed}")
    print(f"  Score  : {pct}%")
    print("=" * 55)

    if failed > 0:
        print("\n  Failed checks:")
        for r in results:
            if not r["passed"]:
                print(f"    → {r['name']}")

    if pct == 100:
        print("\n  🎉 All checks passed!")
    elif pct >= 80:
        print("\n  ⚠️  Mostly healthy. Review failures above.")
    else:
        print("\n  🚨 Critical failures detected!")

    return failed == 0


# ─────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔍 Running Data Quality Checks...")
    print(f"   {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}\n")

    check_users()
    check_orders()
    check_products()
    check_clickstream()
    check_warehouse()
    check_dbt_marts()

    all_passed = print_summary()
    sys.exit(0 if all_passed else 1)