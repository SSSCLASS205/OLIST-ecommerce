import pandas as pd
import os

PATH = "/home/sssclass/Desktop/project/my_project/archive"

orders_olist = pd.read_csv(f'{PATH}/olist_orders_dataset.csv')
order_item   = pd.read_csv(f'{PATH}/olist_order_items_dataset.csv')
customer     = pd.read_csv(f'{PATH}/olist_customers_dataset.csv', dtype={'customer_zip_code_prefix': str})
payment      = pd.read_csv(f'{PATH}/olist_order_payments_dataset.csv')
review       = pd.read_csv(f'{PATH}/olist_order_reviews_dataset.csv')
category     = pd.read_csv(f'{PATH}/product_category_name_translation.csv')
seller       = pd.read_csv(f'{PATH}/olist_sellers_dataset.csv', dtype={'seller_zip_code_prefix': str})
product      = pd.read_csv(f'{PATH}/olist_products_dataset.csv')
geolocation  = pd.read_csv(f'{PATH}/olist_geolocation_dataset.csv', dtype={'geolocation_zip_code_prefix': str})


def save_order_scoped_chunk(folder_name, orders_chunk):
    os.makedirs(folder_name, exist_ok=True)

    order_item_chunk = order_item[order_item['order_id'].isin(orders_chunk["order_id"])]
    review_chunk     = review[review['order_id'].isin(orders_chunk["order_id"])]
    payment_chunk    = payment[payment['order_id'].isin(orders_chunk["order_id"])]

    orders_chunk.to_csv(f'{folder_name}/orders_olist.csv', index=False)
    order_item_chunk.to_csv(f'{folder_name}/order_item.csv', index=False)
    review_chunk.to_csv(f'{folder_name}/review.csv', index=False)
    payment_chunk.to_csv(f'{folder_name}/payment.csv', index=False)

    print(f"✅ Saved {len(orders_chunk)} orders + order-scoped tables to {folder_name}/")


def save_shared_reference_tables(folder_name='../shared_reference'):
    """Dimension tables shared across all parts — saved once, no duplication."""
    os.makedirs(folder_name, exist_ok=True)
    customer.to_csv(f'{folder_name}/customer.csv', index=False)
    seller.to_csv(f'{folder_name}/seller.csv', index=False)
    product.to_csv(f'{folder_name}/product.csv', index=False)
    category.to_csv(f'{folder_name}/category.csv', index=False)
    geolocation.to_csv(f'{folder_name}/geolocation.csv', index=False)
    print(f"✅ Saved shared reference tables to {folder_name}/")


# ==========================================
# PART 1: Non-delivered orders
# ==========================================
print("Processing Part 1...")
status_list = ["created", "approved", "invoiced", "processing", "shipped"]
orders_day1 = orders_olist[orders_olist['order_status'].isin(status_list)]
save_order_scoped_chunk('../part1', orders_day1)

print("Processing Part 2...")
orders_delivered = orders_olist[orders_olist['order_status'] == "delivered"]
save_order_scoped_chunk('../part2', orders_delivered)

# Reference tables saved ONCE, not duplicated per chunk
save_shared_reference_tables()

print("All data successfully sliced and saved!")