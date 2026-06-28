import os
import pandas as pd
from sqlalchemy import create_engine, text

# 1. DATABASE CONNECTION CONFIGURATION
DATABASE_URL = "postgresql://postgres:password@localhost:5432/postgres"
engine = create_engine(DATABASE_URL)

# 2. FILE PATH SETUP
BASE_DIR = "/home/sssclass/Desktop/project/my_project/part1"

csv_mapping = {
    "product_category_name_translation": "category.csv",
    "geolocation": "geolocation.csv",
    "products": "product.csv",
    "customers": "customer.csv",
    "sellers": "seller.csv",
    "orders": "orders_olist.csv",
    "order_items": "order_item.csv",
    "order_payments": "payment.csv",
    "order_reviews": "review.csv"
}

def load_csv(table_name):
    """Helper function to find and read the CSV file"""
    file_path = os.path.join(BASE_DIR, csv_mapping[table_name])
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Missing required CSV file: {file_path}")
    print(f"📖 Reading {csv_mapping[table_name]}...")
    return pd.read_csv(file_path)

# 3. STRICT ORDERED INGESTION PIPELINE
try:
    print("🧹 Cleaning existing data safely from the schema...")
    # This empties the tables in reverse order of constraints without breaking them, 
    # keeping your primary keys, surrogate keys, and sequences completely intact.
    with engine.begin() as conn:
        conn.execute(text("""
            TRUNCATE TABLE olist.order_reviews, olist.order_payments, olist.order_items, olist.orders, 
                           olist.products, olist.product_category_name_translation, olist.sellers, 
                           olist.customers, olist.geolocation RESTART IDENTITY CASCADE;
        """))

    print("🚀 Starting Day 1 Bulk CSV Ingestion into Postgres...")

    # --- LEVEL 1: INDEPENDENT TABLES ---
    
    # Geolocation (Kept as append to preserve the SERIAL primary key sequence!)
    df_geo = load_csv("geolocation")
    df_geo.to_sql("geolocation", con=engine, schema="olist", if_exists="append", index=False)

    # Product Category Translation (Kept as append to preserve its Primary Key)
    df_trans = load_csv("product_category_name_translation")
    df_trans.to_sql("product_category_name_translation", con=engine, schema="olist", if_exists="append", index=False)

    # Products
    df_prod = load_csv("products")
    df_prod = df_prod.rename(columns={
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length"
    })
    df_prod.to_sql("products", con=engine, schema="olist", if_exists="append", index=False)

    # Customers
    df_cust = load_csv("customers")
    df_cust.to_sql("customers", con=engine, schema="olist", if_exists="append", index=False)

    # Sellers
    df_sell = load_csv("sellers")
    df_sell.to_sql("sellers", con=engine, schema="olist", if_exists="append", index=False)


    # --- LEVEL 2: DEPENDENT TABLES (Requires Level 1 Parents) ---

    # Orders (Convert timestamp strings to actual datetime objects)
    df_orders = load_csv("orders")
    timestamp_cols = [
        "order_purchase_timestamp", "order_approved_at", 
        "order_delivered_carrier_date", "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]
    for col in timestamp_cols:
        df_orders[col] = pd.to_datetime(df_orders[col], errors="coerce")
    df_orders.to_sql("orders", con=engine, schema="olist", if_exists="append", index=False)


    # --- LEVEL 3: CHILD TABLES (Requires Orders to exist) ---

    # Order Items
    df_items = load_csv("order_items")
    df_items["shipping_limit_date"] = pd.to_datetime(df_items["shipping_limit_date"], errors="coerce")
    df_items.to_sql("order_items", con=engine, schema="olist", if_exists="append", index=False)

    # Order Payments
    df_payments = load_csv("order_payments")
    df_payments.to_sql("order_payments", con=engine, schema="olist", if_exists="append", index=False)

    # Order Reviews
    df_reviews = load_csv("order_reviews")
    review_time_cols = ["review_creation_date", "review_answer_timestamp"]
    for col in review_time_cols:
        df_reviews[col] = pd.to_datetime(df_reviews[col], errors="coerce")
    df_reviews.to_sql("order_reviews", con=engine, schema="olist", if_exists="append", index=False)

    print("🎉 All Day 1 CSV data successfully loaded into the 'olist' schema!")

except Exception as e:
    print(f"❌ Ingestion stopped due to error: {e}")