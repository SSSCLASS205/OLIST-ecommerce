import json
import uuid
import pandas as pd
import os
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

S3_BUCKET_NAME = "olist-raw-staging-dev-724769809986" 
S3_PREFIX = "raw/" 

# Initialize the S3 client
s3_client = boto3.client('s3')

# ==========================================
# 2. Define the Mock Olist Data (All 9 Tables)
# ==========================================

mock_customers = [
    {"customer_id": "9ef432eb6251297304e76186b10a928d", "customer_unique_id": "7c396fd4830fd04220f754e42b4e5bff", "customer_zip_code_prefix": "03149", "customer_city": "sao paulo", "customer_state": "SP"},
    {"customer_id": "b0830fb4747a6c6d20dea0b8c802d7ef", "customer_unique_id": "af07308b275d755c9edb36a90c618231", "customer_zip_code_prefix": "47813", "customer_city": "barreiras", "customer_state": "BA"}
]

mock_orders = [
    {"order_id": "e481f51cbdc54678b7cc49136f2d6af7", "customer_id": "9ef432eb6251297304e76186b10a928d", "order_status": "delivered", "order_purchase_timestamp": "2017-10-02 10:56:33", "order_approved_at": "2017-10-02 11:07:15", "order_delivered_customer_date": "2017-10-10 21:25:13", "order_estimated_delivery_date": "2017-10-18 00:00:00"},
    {"order_id": "53cdb2fc8bc7dce0b6741e2150273451", "customer_id": "b0830fb4747a6c6d20dea0b8c802d7ef", "order_status": "shipped", "order_purchase_timestamp": "2018-07-24 20:41:37", "order_approved_at": "2018-07-26 03:24:27", "order_delivered_customer_date": None, "order_estimated_delivery_date": "2018-08-13 00:00:00"}
]

mock_order_payments = [
    {"order_id": "e481f51cbdc54678b7cc49136f2d6af7", "payment_sequential": 1, "payment_type": "credit_card", "payment_installments": 1, "payment_value": 18.12},
    {"order_id": "e481f51cbdc54678b7cc49136f2d6af7", "payment_sequential": 2, "payment_type": "voucher", "payment_installments": 1, "payment_value": 2.00},
    {"order_id": "53cdb2fc8bc7dce0b6741e2150273451", "payment_sequential": 1, "payment_type": "boleto", "payment_installments": 1, "payment_value": 141.46}
]

mock_order_reviews = [
    {"review_id": "a54f0611adc9ed256b57ede6b6eb5114", "order_id": "e481f51cbdc54678b7cc49136f2d6af7", "review_score": 4, "review_comment_title": None, "review_comment_message": "Nao testei o produto ainda.", "review_creation_date": "2017-10-11 00:00:00", "review_answer_timestamp": "2017-10-12 03:43:48"},
    {"review_id": "8d5266042046a06655c8db133d120ba5", "order_id": "53cdb2fc8bc7dce0b6741e2150273451", "review_score": 5, "review_comment_title": "Muito bom", "review_comment_message": None, "review_creation_date": "2018-08-08 00:00:00", "review_answer_timestamp": "2018-08-08 18:37:50"}
]

mock_order_items = [
    {"order_id": "e481f51cbdc54678b7cc49136f2d6af7", "order_item_id": 1, "product_id": "87285b34884572647811a353c7ac498a", "seller_id": "3504c0cb71d7fa48d967e0e4c94d59d9", "shipping_limit_date": "2017-10-06 11:07:15", "price": 29.99, "freight_value": 8.72},
    {"order_id": "53cdb2fc8bc7dce0b6741e2150273451", "order_item_id": 1, "product_id": "595fac2a385ac33a80bd5114aec74eb8", "seller_id": "289cdb325fb7e7f891c38608bf9e0962", "shipping_limit_date": "2018-07-30 03:24:27", "price": 118.70, "freight_value": 22.76}
]

mock_products = [
    {"product_id": "87285b34884572647811a353c7ac498a", "product_category_name": "utilidades_domesticas", "product_name_lenght": 40, "product_description_lenght": 268, "product_photos_qty": 4, "product_weight_g": 500, "product_length_cm": 19, "product_height_cm": 8, "product_width_cm": 13},
    {"product_id": "595fac2a385ac33a80bd5114aec74eb8", "product_category_name": "perfumaria", "product_name_lenght": 29, "product_description_lenght": 178, "product_photos_qty": 1, "product_weight_g": 400, "product_length_cm": 19, "product_height_cm": 13, "product_width_cm": 19}
]

mock_sellers = [
    {"seller_id": "3504c0cb71d7fa48d967e0e4c94d59d9", "seller_zip_code_prefix": "09350", "seller_city": "maua", "seller_state": "SP"},
    {"seller_id": "289cdb325fb7e7f891c38608bf9e0962", "seller_zip_code_prefix": "31570", "seller_city": "belo horizonte", "seller_state": "MG"}
]

mock_geolocation = [
    {"geolocation_zip_code_prefix": "03149", "geolocation_lat": -23.574809, "geolocation_lng": -46.587471, "geolocation_city": "sao paulo", "geolocation_state": "SP"},
    {"geolocation_zip_code_prefix": "09350", "geolocation_lat": -23.673803, "geolocation_lng": -46.441999, "geolocation_city": "maua", "geolocation_state": "SP"}
]

mock_category_translation = [
    {"product_category_name": "utilidades_domesticas", "product_category_name_english": "housewares"},
    {"product_category_name": "perfumaria", "product_category_name_english": "perfumery"}
]

tables_to_generate = {
    "customers": mock_customers,
    "orders": mock_orders,
    "order_payments": mock_order_payments,
    "order_reviews": mock_order_reviews,
    "order_items": mock_order_items,
    "products": mock_products,
    "sellers": mock_sellers,
    "geolocation": mock_geolocation,
    "product_category_name_translation": mock_category_translation
}

def upload_to_s3(local_file, bucket, s3_key):
    """Uploads a file to an S3 bucket"""
    try:
        s3_client.upload_file(local_file, bucket, s3_key)
        print(f"☁️  Successfully uploaded {local_file} to s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"Failed to upload {local_file} to S3: {e}")

def write_and_upload_airbyte_parquet(table_name, data_list):
    """Writes data to a Parquet file in Airbyte format and uploads to S3"""
    filename = f"raw_airbyte_{table_name}.parquet"
    
    records = []
    for row in data_list:
        ab_id = str(uuid.uuid4())
        emitted_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + '+00:00'
        
        records.append({
            "_airbyte_ab_id": ab_id,
            "_airbyte_emitted_at": emitted_at,
            "_airbyte_data": row
        })
    
    df = pd.DataFrame(records)
    df.to_parquet(filename, index=False, engine='pyarrow')
    print(f"Generated local file: {filename} ({len(data_list)} records)")
    
    s3_key = f"{S3_PREFIX}{table_name}/{filename}"
    upload_to_s3(filename, S3_BUCKET_NAME, s3_key)
    
    os.remove(filename)


if __name__ == "__main__":
    print(f"Generating and Uploading Fixtures to s3://{S3_BUCKET_NAME}...\n")
    for table_name, data in tables_to_generate.items():
        write_and_upload_airbyte_parquet(table_name, data)
    print("\nDone! All 9 tables have been generated and pushed to S3 as Parquet files.")