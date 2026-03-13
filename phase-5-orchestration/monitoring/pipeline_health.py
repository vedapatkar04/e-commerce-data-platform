"""
E-Commerce Data Platform — Phase 5

Pipeline health monitor — checks all services are alive
and the data pipeline is functioning correctly.

Think of this as a /health endpoint for your
entire data platform.

Run:
    python pipeline_health.py
"""

import os
import sys
import socket
import time
from datetime import datetime

import psycopg2
from kafka import KafkaAdminClient
from kafka.errors import KafkaError

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

SERVICES = {
    "PostgreSQL":   ("127.0.0.1", 5433),
    "pgAdmin":      ("127.0.0.1", 5050),
    "Airflow":      ("127.0.0.1", 8080),
    "Kafka":        ("127.0.0.1", 9092),
    "Kafka UI":     ("127.0.0.1", 8090),
    "Redis":        ("127.0.0.1", 6379),
    "Zookeeper":    ("127.0.0.1", 2181),
}

# ─────────────────────────────────────────────────────
# Result tracker
# ─────────────────────────────────────────────────────
results = []

def record(category: str, name: str, passed: bool, detail: str = ""):
    status = "✅" if passed else "❌"
    results.append({"category": category, "name": name, "passed": passed})
    detail_str = f" — {detail}" if detail else ""
    print(f"  {status}  {name}{detail_str}")


# ─────────────────────────────────────────────────────
# 1. Port checks — is each service reachable?
# ─────────────────────────────────────────────────────
def check_ports():
    print("\n🔌 Service Connectivity")
    for name, (host, port) in SERVICES.items():
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            record("ports", name, True, f":{port}")
        except (socket.timeout, ConnectionRefusedError, OSError):
            record("ports", name, False, f":{port} unreachable")


# ─────────────────────────────────────────────────────
# 2. PostgreSQL checks — schemas and row counts
# ─────────────────────────────────────────────────────
def check_postgres():
    print("\n🗄️  PostgreSQL Data")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # Check each table has expected data
        checks = [
            ("oltp.users",              1000,   "users"),
            ("oltp.products",           500,    "products"),
            ("oltp.orders",             10000,  "orders"),
            ("oltp.clickstream_events", 100000, "clickstream events"),
            ("warehouse.fact_orders",   1,      "warehouse fact rows"),
            ("warehouse.dim_users",     100,    "dim_users rows"),
        ]

        for table, min_rows, label in checks:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            passed = count >= min_rows
            record("postgres", f"{label} ({table})", passed,
                   f"{count:,} rows (min: {min_rows:,})")

        # Check dbt marts exist
        for mart in ["mart_revenue", "mart_funnel", "mart_top_products"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM dbt_dev_marts.{mart}")
                count = cur.fetchone()[0]
                record("postgres", f"dbt mart: {mart}", count > 0,
                       f"{count:,} rows")
            except Exception as e:
                record("postgres", f"dbt mart: {mart}", False, str(e))

        cur.close()
        conn.close()

    except Exception as e:
        record("postgres", "PostgreSQL connection", False, str(e))


# ─────────────────────────────────────────────────────
# 3. Kafka checks — broker and topics
# ─────────────────────────────────────────────────────
def check_kafka():
    print("\n📨 Kafka")
    try:
        admin = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="health_check",
            request_timeout_ms=5000,
        )
        topics = admin.list_topics()
        admin.close()

        record("kafka", "Kafka broker reachable", True,
               f"{len(topics)} topics")

        expected_topic = "clickstream_events"
        has_topic = expected_topic in topics
        record("kafka", f"Topic '{expected_topic}' exists", has_topic)

    except KafkaError as e:
        record("kafka", "Kafka broker", False, str(e))
    except Exception as e:
        record("kafka", "Kafka broker", False, str(e))


# ─────────────────────────────────────────────────────
# 4. Pipeline freshness — when did pipeline last run?
# ─────────────────────────────────────────────────────
def check_freshness():
    print("\n🕐 Data Freshness")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()

        # Check most recent order in warehouse
        cur.execute("""
            SELECT MAX(date_key)
            FROM warehouse.fact_orders
        """)
        row = cur.fetchone()
        if row and row[0]:
            latest = str(row[0])
            record("freshness", "Warehouse has order data", True,
                   f"latest date_key: {latest}")
        else:
            record("freshness", "Warehouse has order data", False,
                   "no data found")

        # Check most recent clickstream event
        cur.execute("""
            SELECT MAX(event_at) FROM oltp.clickstream_events
        """)
        row = cur.fetchone()
        if row and row[0]:
            latest    = row[0]
            age_hours = (datetime.utcnow() - latest).total_seconds() / 3600
            fresh     = age_hours < 24
            record("freshness", "Clickstream events fresh", fresh,
                   f"latest {age_hours:.1f}h ago")
        else:
            record("freshness", "Clickstream events exist", False)

        cur.close()
        conn.close()

    except Exception as e:
        record("freshness", "Freshness check", False, str(e))


# ─────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────
def print_summary():
    total  = len(results)
    passed = sum(1 for r in results if r["passed"])
    failed = total - passed
    pct    = round(100 * passed / total) if total > 0 else 0

    print("\n" + "=" * 55)
    print(f"  PIPELINE HEALTH REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    # Summary by category
    categories = {}
    for r in results:
        cat = r["category"]
        if cat not in categories:
            categories[cat] = {"passed": 0, "total": 0}
        categories[cat]["total"] += 1
        if r["passed"]:
            categories[cat]["passed"] += 1

    for cat, stats in categories.items():
        bar = "✅" * stats["passed"] + "❌" * (stats["total"] - stats["passed"])
        print(f"  {cat:<15} {stats['passed']}/{stats['total']}  {bar}")

    print("-" * 55)
    print(f"  Overall: {passed}/{total} checks passed ({pct}%)")
    print("=" * 55)

    if pct == 100:
        print("\n  🎉 Platform fully healthy!")
    elif pct >= 80:
        print("\n  ⚠️  Platform mostly healthy. Review failures above.")
    else:
        print("\n  🚨 Critical issues detected!")

    return failed == 0


# ─────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────
def run_health_check():
    print("🏥 E-Commerce Data Platform — Health Check")
    print(f"   Running at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    check_ports()
    check_postgres()
    check_kafka()
    check_freshness()

    all_passed = print_summary()
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    run_health_check()