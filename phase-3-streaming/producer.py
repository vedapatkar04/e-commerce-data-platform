"""
E-Commerce Data Platform — Phase 3
File: phase-3-streaming/producer.py

Kafka Producer — simulates real-time user clickstream events.
Fetches real user/product IDs from PostgreSQL so foreign
key constraints are satisfied in the consumer.

Run:
    python producer.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime

import psycopg2
from faker import Faker
from kafka import KafkaProducer

# ─────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC_NAME     = "clickstream_events"
EVENTS_PER_SEC = 2

fake = Faker()

DB_CONFIG = {
    "host":     os.getenv("ECOMMERCE_DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("ECOMMERCE_DB_PORT", "5433")),
    "dbname":   os.getenv("ECOMMERCE_DB_NAME", "ecommerce_db"),
    "user":     os.getenv("ECOMMERCE_DB_USER", "deuser"),
    "password": os.getenv("ECOMMERCE_DB_PASSWORD", "depass"),
}

EVENT_TYPES   = ["page_view", "product_view", "add_to_cart",
                 "remove_from_cart", "checkout", "purchase"]
EVENT_WEIGHTS = [40, 30, 15, 5, 6, 4]


# ─────────────────────────────────────────────────────
# Load real IDs from PostgreSQL
# ─────────────────────────────────────────────────────
def load_ids_from_db():
    """
    Fetch real user and product IDs from the database.
    This ensures producer events don't violate FK constraints.
    Key concept: producers in real systems pull valid entity
    IDs from a registry or config service.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    cur.execute("SELECT user_id FROM oltp.users LIMIT 500")
    user_ids = [str(row[0]) for row in cur.fetchall()]

    cur.execute("SELECT product_id FROM oltp.products LIMIT 200")
    product_ids = [str(row[0]) for row in cur.fetchall()]

    cur.close()
    conn.close()

    print(f"✅ Loaded {len(user_ids):,} users and {len(product_ids):,} products from DB")
    return user_ids, product_ids


# ─────────────────────────────────────────────────────
# Serializer
# ─────────────────────────────────────────────────────
def json_serializer(data):
    return json.dumps(data).encode("utf-8")


# ─────────────────────────────────────────────────────
# Event generator
# ─────────────────────────────────────────────────────
def generate_event(user_ids, product_ids) -> dict:
    return {
        "event_id":   str(uuid.uuid4()),
        "user_id":    random.choice(user_ids),
        "product_id": random.choice(product_ids),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
        "session_id": str(uuid.uuid4()),
        "event_at":   datetime.utcnow().isoformat(),
        "metadata": {
            "device":   random.choice(["mobile", "desktop", "tablet"]),
            "country":  fake.country(),
            "referrer": random.choice(["google", "direct", "social", "email"]),
        }
    }


# ─────────────────────────────────────────────────────
# Producer
# ─────────────────────────────────────────────────────
def run_producer():
    print(f"🚀 Starting Kafka producer")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Topic:  {TOPIC_NAME}")
    print(f"   Rate:   {EVENTS_PER_SEC} events/sec")
    print(f"   Press Ctrl+C to stop\n")

    # Load real IDs before producing
    user_ids, product_ids = load_ids_from_db()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=json_serializer,
        acks="all",
        retries=3,
        max_block_ms=5000,
    )

    events_sent = 0
    try:
        while True:
            event = generate_event(user_ids, product_ids)

            producer.send(
                TOPIC_NAME,
                key=event["user_id"].encode("utf-8"),
                value=event
            )

            events_sent += 1
            print(f"  ✅ [{events_sent:05d}] {event['event_type']:<18} "
                  f"user: {event['user_id'][:8]}...  "
                  f"at: {event['event_at']}")

            time.sleep(1 / EVENTS_PER_SEC)

    except KeyboardInterrupt:
        print(f"\n⛔ Producer stopped. Total sent: {events_sent:,}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer closed cleanly.")


if __name__ == "__main__":
    run_producer()