{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH bronze_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        _airbyte_emitted_at 
    FROM {{ source('BRONZE', 'orders_bronze') }} 
    
    {% if is_incremental() %}
      WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

bronze_customers AS (
    SELECT
        customer_id,
        customer_unique_id
    FROM {{ source('BRONZE', 'customers_bronze') }}
),

orders_with_customer AS (
    SELECT
        o.*,
        c.customer_unique_id
    FROM bronze_orders o
    LEFT JOIN bronze_customers c ON o.customer_id = c.customer_id
),

ranked_orders AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY _airbyte_emitted_at DESC) as rnk
    FROM orders_with_customer
),

deduped AS (
    SELECT * FROM ranked_orders
    WHERE rnk = 1
)   

SELECT 
    order_id,
    customer_id,
    customer_unique_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    _airbyte_emitted_at 
FROM deduped