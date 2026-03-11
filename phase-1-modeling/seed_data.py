"""
Run:
    pip install psycopg2-binary faker
    python seed_data.py
"""

import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from faker import Faker


# Config
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "admin123",
}

fake = Faker()
random.seed(42)
Faker.seed(42)

# Volume 
NUM_USERS      = 1_000
NUM_PRODUCTS   = 500
NUM_ORDERS     = 10_000
NUM_EVENTS     = 100_000   # clickstream events

# Date range: last 2 years
DATE_START = datetime.now() - timedelta(days=730)
DATE_END   = datetime.now()

# Helpers
def random_date(start=DATE_START, end=DATE_END):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def price_band(price: float) -> str:
    if price < 30:
        return "budget"
    elif price < 150:
        return "mid"
    return "premium"


def user_segment(order_count: int) -> str:
    if order_count == 0:
        return "new"
    elif order_count >= 10:
        return "vip"
    return "regular"


def region_for(country: str) -> str:
    regions = {
        "United States": "North America",
        "Canada": "North America",
        "Mexico": "North America",
        "United Kingdom": "Europe",
        "Germany": "Europe",
        "France": "Europe",
        "Italy": "Europe",
        "Spain": "Europe",
        "India": "Asia",
        "China": "Asia",
        "Japan": "Asia",
        "Australia": "Oceania",
        "Brazil": "South America",
        "Argentina": "South America",
    }
    return regions.get(country, "Other")



# Main seeder
def seed():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Connected to PostgreSQL\n")

    # 1. Categories
    print("📦 Seeding categories...")
    categories = [
        (1, "Electronics",  None),
        (2, "Phones",       1),
        (3, "Laptops",      1),
        (4, "Clothing",     None),
        (5, "Men",          4),
        (6, "Women",        4),
        (7, "Home & Garden",None),
        (8, "Kitchen",      7),
        (9, "Furniture",    7),
        (10,"Sports",       None),
        (11,"Outdoors",     10),
        (12,"Fitness",      10),
    ]
    cur.executemany(
        """
        INSERT INTO oltp.categories (category_id, name, parent_category_id)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        categories
    )
    conn.commit()
    category_ids = [c[0] for c in categories]
    print(f"   → {len(categories)} categories inserted")
    

    # 2. Products
    print("📦 Seeding products...")
    product_ids = []
    product_prices = {}
    products_batch = []

    for _ in range(NUM_PRODUCTS):
        pid      = uuid.uuid4()
        cat_id   = random.choice(category_ids)
        name     = fake.catch_phrase()
        price    = round(random.uniform(5.0, 800.0), 2)
        stock    = random.randint(0, 500)
        created  = random_date()

        product_ids.append(pid)
        product_prices[pid] = price
        products_batch.append((str(pid), cat_id, name, price, stock, created))

    cur.executemany(
        """
        INSERT INTO oltp.products (product_id, category_id, name, price, stock_qty, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        products_batch
    )
    conn.commit()
    print(f"   → {NUM_PRODUCTS} products inserted")    


    # 3. Users
    print("👤 Seeding users...")
    user_ids      = []
    user_country  = {}
    users_batch   = []
    countries = [
        "United States", "United Kingdom", "Germany", "India",
        "Canada", "France", "Australia", "Brazil", "Japan", "Mexico"
    ]

    for _ in range(NUM_USERS):
        uid      = uuid.uuid4()
        email    = fake.unique.email()
        name     = fake.name()
        country  = random.choice(countries)
        created  = random_date()
        active   = random.random() > 0.05   # 95% active

        user_ids.append(uid)
        user_country[uid] = country
        users_batch.append((str(uid), email, name, country, created, active))

    cur.executemany(
        """
        INSERT INTO oltp.users (user_id, email, full_name, country, created_at, is_active)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        users_batch
    )
    conn.commit()
    print(f"   → {NUM_USERS} users inserted")

    # 4. Orders + Order Items
    print("🧾 Seeding orders and order items...")
    statuses      = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    status_weights= [5, 10, 15, 65, 5]   # realistic distribution
    user_order_count = {uid: 0 for uid in user_ids}

    orders_batch      = []
    order_items_batch = []

    for _ in range(NUM_ORDERS):
        oid        = uuid.uuid4()
        user       = random.choice(user_ids)
        status     = random.choices(statuses, weights=status_weights)[0]
        created    = random_date()
        shipped    = created + timedelta(days=random.randint(1, 5)) if status in ('shipped', 'delivered') else None

        # 1-5 items per order
        num_items  = random.randint(1, 5)
        items      = random.sample(product_ids, min(num_items, len(product_ids)))
        total      = 0.0

        for prod in items:
            qty        = random.randint(1, 4)
            unit_price = product_prices[prod]
            discount   = round(random.choice([0, 0, 0, 5, 10, 15, 20]), 2)
            total     += unit_price * qty * (1 - discount / 100)
            order_items_batch.append((
                str(oid), str(prod), qty, unit_price, discount
            ))

        orders_batch.append((str(oid), str(user), status, round(total, 2), created, shipped))
        user_order_count[user] += 1

    cur.executemany(
        """
        INSERT INTO oltp.orders (order_id, user_id, status, total_amount, created_at, shipped_at)
        VALUES (%s, %s, %s::oltp.order_status, %s, %s, %s)
        """,
        orders_batch
    )
    cur.executemany(
        """
        INSERT INTO oltp.order_items (order_id, product_id, quantity, unit_price, discount)
        VALUES (%s, %s, %s, %s, %s)
        """,
        order_items_batch
    )
    conn.commit()
    print(f"   → {NUM_ORDERS} orders inserted")
    print(f"   → {len(order_items_batch)} order items inserted")

    # 5. Clickstream Events
    print("🖱️  Seeding clickstream events...")
    event_types   = ['page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'checkout', 'purchase']
    event_weights = [40, 30, 15, 5, 6, 4]   # realistic funnel drop-off
    events_batch  = []

    for _ in range(NUM_EVENTS):
        eid        = uuid.uuid4()
        user       = random.choice(user_ids)
        product    = random.choice(product_ids)
        event      = random.choices(event_types, weights=event_weights)[0]
        session    = uuid.uuid4()
        event_at   = random_date()

        events_batch.append((str(eid), str(user), str(product), event, str(session), event_at))

        # Batch insert every 5000 rows for performance
        if len(events_batch) % 5000 == 0:
            cur.executemany(
                """
                INSERT INTO oltp.clickstream_events
                    (event_id, user_id, product_id, event_type, session_id, event_at)
                VALUES (%s, %s, %s, %s::oltp.event_type, %s, %s)
                """,
                events_batch
            )
            conn.commit()
            events_batch = []
            print(f"   → batch committed...")

    # Insert remaining
    if events_batch:
        cur.executemany(
            """
            INSERT INTO oltp.clickstream_events
                (event_id, user_id, product_id, event_type, session_id, event_at)
            VALUES (%s, %s, %s, %s::oltp.event_type, %s, %s)
            """,
            events_batch
        )
        conn.commit()

    print(f"   → {NUM_EVENTS} clickstream events inserted")

    # 6. Summary
    print("\n" + "="*50)
    print("✅ SEED COMPLETE")
    print("="*50)
    cur.execute("SELECT COUNT(*) FROM oltp.users")
    print(f"   users:             {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM oltp.products")
    print(f"   products:          {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM oltp.orders")
    print(f"   orders:            {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM oltp.order_items")
    print(f"   order_items:       {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM oltp.clickstream_events")
    print(f"   clickstream_events:{cur.fetchone()[0]:,}")
    print("="*50)
    print("\n🔗 Connect via DBeaver: localhost:5432 / ecommerce_db")
    print("🌐 pgAdmin UI: http://localhost:5050\n")

    cur.close()
    conn.close()


if __name__ == "__main__":
    seed()
    