"""
E-Commerce Data Platform — Phase 2

Transform layer — cleans and reshapes raw data
into warehouse-ready dimension and fact formats.

Key concept: Transform is where business logic lives.
No raw data goes directly into the warehouse.
"""

import pandas as pd


# ─────────────────────────────────────────────────────
# Lookup table: country → region
# In a real pipeline this might come from a config file
# ─────────────────────────────────────────────────────
REGION_MAP = {
    "United States": "North America",
    "Canada":        "North America",
    "Mexico":        "North America",
    "United Kingdom":"Europe",
    "Germany":       "Europe",
    "France":        "Europe",
    "Italy":         "Europe",
    "Spain":         "Europe",
    "India":         "Asia",
    "China":         "Asia",
    "Japan":         "Asia",
    "Australia":     "Oceania",
    "Brazil":        "South America",
    "Argentina":     "South America",
}


def transform_users(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw users into dim_users format.

    Key transformation: derive 'segment' from order history.
    We use registered_at to estimate — in a real pipeline
    you'd join with order counts.

    segment logic:
      new     → registered in last 30 days
      vip     → registered 2+ years ago (assumed loyal)
      regular → everyone else
    """
    df = df.copy()

    # Ensure date column is datetime
    df["registered_at"] = pd.to_datetime(df["registered_at"])

    # Derive segment based on registration date
    today = pd.Timestamp.now()
    df["segment"] = "regular"
    df.loc[(today - df["registered_at"]).dt.days <= 30,   "segment"] = "new"
    df.loc[(today - df["registered_at"]).dt.days >= 730,  "segment"] = "vip"

    # Keep only warehouse columns
    df = df[["user_id", "full_name", "country", "segment", "registered_at"]]

    # Drop rows with no country (data quality)
    df = df.dropna(subset=["country"])

    print(f"[TRANSFORM] dim_users: {len(df):,} rows")
    return df


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw products into dim_products format.

    Key transformation: derive 'price_band' from price.
    This lets analysts filter by price tier without
    knowing exact price ranges.
    """
    df = df.copy()

    # Derive price band
    df["price_band"] = pd.cut(
        df["price"],
        bins=[0, 30, 150, float("inf")],
        labels=["budget", "mid", "premium"]
    ).astype(str)

    # Keep only warehouse columns
    df = df[["product_id", "name", "category", "price_band"]]

    # Fill nulls in category (products without category)
    df["category"] = df["category"].fillna("Uncategorized")

    print(f"[TRANSFORM] dim_products: {len(df):,} rows")
    return df


def transform_locations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform unique countries into dim_location format.
    Derives region from country using our lookup table.
    """
    df = df.copy()

    df["region"] = df["country"].map(REGION_MAP).fillna("Other")

    print(f"[TRANSFORM] dim_location: {len(df):,} rows")
    return df


def transform_orders(
    orders_df:   pd.DataFrame,
    users_df:    pd.DataFrame,
    products_df: pd.DataFrame,
    locations_df:pd.DataFrame,
) -> pd.DataFrame:
    """
    Transform raw orders into fact_orders format.

    This is the most important transformation:
    1. Calculate total_revenue per line item
    2. Derive date_key (YYYYMMDD integer)
    3. Join surrogate keys from dimension tables

    Key concept: The fact table stores FK integers
    (surrogate keys), NOT the original UUIDs.
    This makes warehouse JOINs much faster.
    """
    df = orders_df.copy()

    # ── 1. Calculate measures ──────────────────────────
    # Revenue = (price × qty) minus discount percentage
    df["discount_amt"] = (df["unit_price"] * df["quantity"]) * (df["discount"] / 100)
    df["total_revenue"] = (df["unit_price"] * df["quantity"]) - df["discount_amt"]
    df["total_revenue"] = df["total_revenue"].round(2)
    df["discount_amt"]  = df["discount_amt"].round(2)

    # ── 2. Derive date_key ────────────────────────────
    # Convert timestamp to YYYYMMDD integer
    # e.g. 2024-03-15 → 20240315
    df["created_at"] = pd.to_datetime(df["created_at"])
    df["date_key"]   = df["created_at"].dt.strftime("%Y%m%d").astype(int)

    # ── 3. Add surrogate keys via merge ───────────────
    # user_key: merge on user_id
    users_keys = users_df[["user_id"]].reset_index()
    users_keys.columns = ["user_key", "user_id"]
    users_keys["user_key"] += 1
    df = df.merge(users_keys, on="user_id", how="left")

    # product_key: merge on product_id
    products_keys = products_df[["product_id"]].reset_index()
    products_keys.columns = ["product_key", "product_id"]
    products_keys["product_key"] += 1
    df = df.merge(products_keys, on="product_id", how="left")

    # location_key: merge via country (join users for country)
    users_country = users_df[["user_id", "country"]]
    df = df.merge(users_country, on="user_id", how="left")

    locations_keys = locations_df[["country"]].reset_index()
    locations_keys.columns = ["location_key", "country"]
    locations_keys["location_key"] += 1
    df = df.merge(locations_keys, on="country", how="left")

    # ── 4. Select final fact columns ──────────────────
    df = df[[
        "user_key",
        "product_key",
        "date_key",
        "location_key",
        "order_id",
        "quantity",
        "unit_price",
        "discount_amt",
        "total_revenue",
        "order_status",
    ]]

    # Drop rows where any key is null (data quality guard)
    df = df.dropna(subset=["user_key", "product_key", "date_key"])

    # Convert keys to int
    df["user_key"]     = df["user_key"].astype(int)
    df["product_key"]  = df["product_key"].astype(int)
    df["location_key"] = df["location_key"].fillna(0).astype(int)

    print(f"[TRANSFORM] fact_orders: {len(df):,} rows")
    return df