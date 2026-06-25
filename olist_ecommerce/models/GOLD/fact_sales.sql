{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}



WITH orders_silver AS(
    SELECT * FROM {{ref('orders_silver')}}    
),
order_items_silver AS (
        SELECT * FROM {{ref('orders_items_silver')}}
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),
joined_data AS (
    SELECT 
        a.order_id,
        b.order_item_id,
        a.order_status,
        b.product_id,
        b.seller_id,
        a.customer_unique_id,
        b.price,
        b.freight_value,
        a.order_purchase_timestamp,
        b._airbyte_emitted_at
    FROM orders_silver a
    join order_items_silver b on  a.order_id = b.order_id
)

SELECT  
    order_id,
    order_item_id,
    order_status,
    customer_unique_id,
    seller_id,
    product_id,
    REPLACE(CAST(order_purchase_timestamp as DATE)::VARCHAR,'-','') as purchase_date_id,
    price,
    freight_value,
    _airbyte_emitted_at
FROM
    joined_data