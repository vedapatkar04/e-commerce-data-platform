"""
E-Commerce Data Platform — Phase 3

Kafka Consumer — reads clickstream events from Kafka
and writes them to PostgreSQL in micro-batches.

Key concept: Consumers read from a topic and process
messages. They track their position using 'offsets'
— like a bookmark in the event stream.

Run (in a separate terminal from producer):
    python consumer.py
"""

import json
import os
from datetime import datetime

import psycopg2
from kafka import KafkaConsumer

# ─────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"
TOPIC_NAME      = "clickstream_events"
CONSUMER_GROUP  = "ecommerce-clickstream-group"
BATCH_SIZE      = 50    # write to DB every 50 messages

DB_CONFIG = {
    "host":     os.getenv("ECOMMERCE_DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("ECOMMERCE_DB_PORT", "5433")),
    "dbname":   os.getenv("ECOMMERCE_DB_NAME", "ecommerce_db"),
    "user":     os.getenv("ECOMMERCE_DB_USER", "deuser"),
    "password": os.getenv("ECOMMERCE_DB_PASSWORD", "depass"),
}


# ─────────────────────────────────────────────────────
# Database writer
# ─────────────────────────────────────────────────────
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def write_batch_to_db(events: list, conn):
    """
    Write a batch of events to PostgreSQL.
    Uses executemany for performance — one round trip
    for the entire batch instead of one per event.

    Key concept: micro-batching — we don't write every
    single event immediately. We buffer N events then
    flush. Balances latency vs throughput.
    """
    if not events:
        return

    cur = conn.cursor()
    try:
        cur.executemany("""
            INSERT INTO oltp.clickstream_events
                (event_id, user_id, product_id, event_type, session_id, event_at)
            VALUES (%s, %s, %s, %s::oltp.event_type, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, [
            (
                e["event_id"],
                e["user_id"],
                e["product_id"],
                e["event_type"],
                e["session_id"],
                e["event_at"],
            )
            for e in events
        ])
        conn.commit()
        print(f"  💾 Batch written: {len(events)} events → PostgreSQL")
    except Exception as ex:
        conn.rollback()
        print(f"  ❌ DB write failed: {ex}")
    finally:
        cur.close()


# ─────────────────────────────────────────────────────
# Consumer
# ─────────────────────────────────────────────────────
def run_consumer():
    print(f"🎧 Starting Kafka consumer")
    print(f"   Broker:  {KAFKA_BROKER}")
    print(f"   Topic:   {TOPIC_NAME}")
    print(f"   Group:   {CONSUMER_GROUP}")
    print(f"   Batch:   {BATCH_SIZE} events")
    print(f"   Press Ctrl+C to stop\n")

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        group_id=CONSUMER_GROUP,
        # Start from earliest unread message
        # "earliest" = read all messages from beginning if new group
        # "latest"   = only read new messages going forward
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # Poll settings
        max_poll_records=BATCH_SIZE,
        session_timeout_ms=30000,
    )

    conn = get_db_connection()
    print(f"✅ Connected to PostgreSQL\n")

    batch        = []
    total_events = 0
    event_counts = {}   # track event type distribution

    try:
        for message in consumer:
            event = message.value

            # Validate event has required fields
            if not all(k in event for k in
                       ["event_id", "user_id", "product_id",
                        "event_type", "session_id", "event_at"]):
                print(f"  ⚠️  Skipping malformed event: {event}")
                continue

            # Only process known event types (data quality guard)
            valid_types = {"page_view", "product_view", "add_to_cart",
                           "remove_from_cart", "checkout", "purchase"}
            if event["event_type"] not in valid_types:
                print(f"  ⚠️  Unknown event type: {event['event_type']}")
                continue

            batch.append(event)
            total_events += 1

            # Track distribution
            evt = event["event_type"]
            event_counts[evt] = event_counts.get(evt, 0) + 1

            print(f"  📥 [{total_events:05d}] "
                  f"{event['event_type']:<18} "
                  f"offset: {message.offset}")

            # Flush batch when it reaches BATCH_SIZE
            if len(batch) >= BATCH_SIZE:
                write_batch_to_db(batch, conn)
                batch = []

                # Print event distribution summary
                print(f"\n  📊 Event distribution so far:")
                for etype, count in sorted(
                    event_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                ):
                    bar = "█" * (count // 5)
                    print(f"     {etype:<20} {count:>5}  {bar}")
                print()

    except KeyboardInterrupt:
        print(f"\n⛔ Consumer stopped.")
        # Write any remaining events in buffer
        if batch:
            write_batch_to_db(batch, conn)
            print(f"✅ Flushed remaining {len(batch)} events")
        print(f"✅ Total events processed: {total_events:,}")
    finally:
        consumer.close()
        conn.close()
        print("✅ Consumer closed cleanly.")


if __name__ == "__main__":
    run_consumer()