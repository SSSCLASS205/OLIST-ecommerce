"""
olist_simulator.py

Seeds RDS with the historical slice of the Olist dataset, then replays the
"future" slice as live INSERT/UPDATE traffic — so you can watch Airbyte's
CDC connector pick up real-time changes after the initial sync.

Flow:
  1. Load all 9 source CSVs.
  2. Split orders (and their related facts) at SPLIT_DATE into
     history vs. live.
  3. Bulk-load dimensions (100%) + historical facts into Postgres.
  4. Add primary keys / replica identity to every table so logical
     replication (CDC) can actually emit UPDATE/DELETE events later —
     tables Airbyte publishes with no replica identity will reject
     UPDATEs outright.
  5. Pause for you to run Airbyte's first (snapshot) sync.
  6. Replay live orders one at a time: INSERT as "processing", pause,
     UPDATE to "shipped" — the UPDATE is what proves CDC is working.

Run enable_cdc_setup.py first (creates the Airbyte role + publication)
before running this script.
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

try:
    from tqdm import tqdm
except ImportError:  # optional dependency — fall back to a no-op wrapper
    def tqdm(iterable, **kwargs):
        return iterable


# ==========================================
# CONFIGURATION
# ==========================================
load_dotenv(find_dotenv())

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")

ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "../archive")
SPLIT_DATE = os.getenv("SPLIT_DATE", "2018-07-01")

# Tables that get a real replica identity so CDC can replicate UPDATEs.
# order_items / order_payments / order_reviews all use composite keys —
# no single column on any of them is unique on its own.
PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    # review_id alone isn't unique (a review token can be resent and land
    # against a second order), but (review_id, order_id) together is —
    # confirmed against the actual CSV. Composite PK beats REPLICA IDENTITY
    # FULL: replication only needs to log these two small columns instead
    # of the whole old row on every UPDATE/DELETE.
    "order_reviews": ["review_id", "order_id"],
}
REPLICA_IDENTITY_FULL_TABLES = ["geolocation", "product_category_name_translation"]

MIN_PROCESSING_PAUSE, MAX_PROCESSING_PAUSE = 1.0, 2.5
MIN_ORDER_PAUSE, MAX_ORDER_PAUSE = 1.0, 3.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("olist_simulator")


def build_conn_str() -> str:
    missing = [name for name, val in [("DB_HOST", DB_HOST), ("DB_NAME", DB_NAME),
                                       ("DB_USER", DB_USER), ("DB_PASS", DB_PASS)] if not val]
    if missing:
        raise EnvironmentError(f"Missing required .env variables: {', '.join(missing)}")
    # quote_plus so special characters in the password (@, :, /, #...) don't
    # break URL parsing or silently point the connection at the wrong host.
    return f"postgresql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def clean_val(val):
    """Convert pandas/numpy NaN to a real SQL NULL."""
    return None if pd.isna(val) else val


# ==========================================
# STEP 1 — LOAD & SPLIT
# ==========================================
def load_csvs() -> dict[str, pd.DataFrame]:
    log.info("Loading all 9 CSVs from '%s'...", ARCHIVE_DIR)
    frames = {
        "customers": pd.read_csv(f"{ARCHIVE_DIR}/olist_customers_dataset.csv"),
        "geolocation": pd.read_csv(f"{ARCHIVE_DIR}/olist_geolocation_dataset.csv"),
        "products": pd.read_csv(f"{ARCHIVE_DIR}/olist_products_dataset.csv"),
        "sellers": pd.read_csv(f"{ARCHIVE_DIR}/olist_sellers_dataset.csv"),
        "translation": pd.read_csv(f"{ARCHIVE_DIR}/product_category_name_translation.csv"),
        "orders": pd.read_csv(f"{ARCHIVE_DIR}/olist_orders_dataset.csv"),
        "items": pd.read_csv(f"{ARCHIVE_DIR}/olist_order_items_dataset.csv"),
        "payments": pd.read_csv(f"{ARCHIVE_DIR}/olist_order_payments_dataset.csv"),
        "reviews": pd.read_csv(f"{ARCHIVE_DIR}/olist_order_reviews_dataset.csv"),
    }
    log.info("Loaded %s rows of orders, %s rows of geolocation.",
              len(frames["orders"]), len(frames["geolocation"]))
    return frames


def split_history_vs_live(frames: dict[str, pd.DataFrame]) -> tuple[dict, dict]:
    log.info("Splitting facts at %s based on order_purchase_timestamp...", SPLIT_DATE)

    orders = frames["orders"].copy()
    orders["order_purchase_timestamp"] = pd.to_datetime(orders["order_purchase_timestamp"])

    history_orders = orders[orders["order_purchase_timestamp"] < SPLIT_DATE]
    live_orders = orders[orders["order_purchase_timestamp"] >= SPLIT_DATE].sort_values(
        "order_purchase_timestamp"
    )

    def split_related(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        hist_ids, live_ids = history_orders["order_id"], live_orders["order_id"]
        return df[df["order_id"].isin(hist_ids)], df[df["order_id"].isin(live_ids)]

    history_items, live_items = split_related(frames["items"])
    history_payments, live_payments = split_related(frames["payments"])
    history_reviews, live_reviews = split_related(frames["reviews"])

    log.info("History: %s orders | Live replay: %s orders", len(history_orders), len(live_orders))

    history = {
        "orders": history_orders, "items": history_items,
        "payments": history_payments, "reviews": history_reviews,
    }
    live = {
        "orders": live_orders, "items": live_items,
        "payments": live_payments, "reviews": live_reviews,
    }
    return history, live


# ==========================================
# STEP 2 — BULK LOAD + CDC-READY SCHEMA
# ==========================================
def bulk_load_historical(engine, frames: dict, history: dict) -> None:
    log.info("Pushing dimension tables (100%% of data)...")
    frames["customers"].to_sql("customers", engine, if_exists="replace", index=False)
    frames["geolocation"].to_sql(
        "geolocation", engine, if_exists="replace", index=False, chunksize=10_000, method="multi"
    )
    frames["products"].to_sql("products", engine, if_exists="replace", index=False)
    frames["sellers"].to_sql("sellers", engine, if_exists="replace", index=False)
    frames["translation"].to_sql(
        "product_category_name_translation", engine, if_exists="replace", index=False
    )

    log.info("Pushing historical facts (past data)...")
    history["orders"].to_sql("orders", engine, if_exists="replace", index=False)
    history["items"].to_sql("order_items", engine, if_exists="replace", index=False)
    history["payments"].to_sql("order_payments", engine, if_exists="replace", index=False)
    history["reviews"].to_sql("order_reviews", engine, if_exists="replace", index=False)

    log.info("Historical load complete.")


def make_tables_cdc_ready(engine) -> None:
    """
    pandas.to_sql(if_exists='replace') creates plain heap tables with no
    primary key. Logical replication (pgoutput) needs a replica identity
    before it will emit UPDATE/DELETE events — without this, the live
    'shipped' status UPDATE later in this script will fail against a
    table that's part of an active publication.
    """
    log.info("Adding primary keys / replica identity for CDC...")
    with engine.begin() as conn:
        for table, cols in PRIMARY_KEYS.items():
            col_list = ", ".join(cols)
            conn.execute(text(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({col_list})'))
        for table in REPLICA_IDENTITY_FULL_TABLES:
            conn.execute(text(f'ALTER TABLE "{table}" REPLICA IDENTITY FULL'))
    log.info("CDC-ready schema in place.")


# ==========================================
# STEP 3 — LIVE TRAFFIC SIMULATOR
# ==========================================
def replay_live_orders(conn_str: str, live: dict) -> None:
    log.info("Starting live traffic simulator (%s orders to replay)...", len(live["orders"]))

    with psycopg2.connect(conn_str) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            for _, order in tqdm(live["orders"].iterrows(), total=len(live["orders"]), desc="Orders"):
                order_id = order["order_id"]
                _insert_order(cursor, order)
                _insert_items(cursor, live["items"], order_id)
                _insert_payments(cursor, live["payments"], order_id)
                _insert_reviews(cursor, live["reviews"], order_id)

                time.sleep(random.uniform(MIN_PROCESSING_PAUSE, MAX_PROCESSING_PAUSE))
                _mark_shipped(cursor, order_id)

                time.sleep(random.uniform(MIN_ORDER_PAUSE, MAX_ORDER_PAUSE))

    log.info("Live traffic simulation complete.")


def _insert_order(cursor, order) -> None:
    log.info("New order: %s", order["order_id"])
    cursor.execute(
        """
        INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp)
        VALUES (%s, %s, 'processing', %s)
        """,
        (order["order_id"], order["customer_id"], order["order_purchase_timestamp"]),
    )


def _insert_items(cursor, live_items: pd.DataFrame, order_id: str) -> None:
    for _, item in live_items[live_items["order_id"] == order_id].iterrows():
        cursor.execute(
            """
            INSERT INTO order_items
                (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                item["order_id"], item["order_item_id"], item["product_id"], item["seller_id"],
                clean_val(item["shipping_limit_date"]), item["price"], item["freight_value"],
            ),
        )


def _insert_payments(cursor, live_payments: pd.DataFrame, order_id: str) -> None:
    for _, payment in live_payments[live_payments["order_id"] == order_id].iterrows():
        cursor.execute(
            """
            INSERT INTO order_payments
                (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                payment["order_id"], payment["payment_sequential"], payment["payment_type"],
                payment["payment_installments"], payment["payment_value"],
            ),
        )


def _insert_reviews(cursor, live_reviews: pd.DataFrame, order_id: str) -> None:
    # Reviews usually land later in real life, but we insert them here
    # up front to keep the simulation simple.
    for _, review in live_reviews[live_reviews["order_id"] == order_id].iterrows():
        cursor.execute(
            """
            INSERT INTO order_reviews
                (review_id, order_id, review_score, review_comment_title,
                 review_comment_message, review_creation_date, review_answer_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                review["review_id"], review["order_id"], review["review_score"],
                clean_val(review["review_comment_title"]), clean_val(review["review_comment_message"]),
                clean_val(review["review_creation_date"]), clean_val(review["review_answer_timestamp"]),
            ),
        )


def _mark_shipped(cursor, order_id: str) -> None:
    log.info("Order shipped: %s", order_id)
    cursor.execute(
        """
        UPDATE orders
        SET order_status = 'shipped', order_delivered_carrier_date = %s
        WHERE order_id = %s
        """,
        (datetime.now(), order_id),
    )


# ==========================================
# MAIN
# ==========================================
def main() -> None:
    conn_str = build_conn_str()
    engine = create_engine(conn_str)

    frames = load_csvs()
    history, live = split_history_vs_live(frames)

    bulk_load_historical(engine, frames, history)
    make_tables_cdc_ready(engine)

    log.info("Historical load + CDC-ready schema complete.")
    input("\n>>> Go run Airbyte's first sync now. Press Enter once it's switched to CDC mode to start live traffic...\n")

    replay_live_orders(conn_str, live)


if __name__ == "__main__":
    main()