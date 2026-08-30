{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'order_item_id']
) }}

WITH changed_order_items AS (
    SELECT DISTINCT order_id
    FROM {{ ref('order_items_silver') }}
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

changed_orders AS (
    SELECT DISTINCT order_id
    FROM {{ ref('orders_silver') }}
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

affected_orders AS (
    SELECT order_id FROM changed_order_items
    UNION
    SELECT order_id FROM changed_orders
),

joined_data AS (
    SELECT
        b.order_id,
        b.order_item_id,
        b.product_id,
        b.seller_id,
        b.shipping_limit_date,
        b.price,
        b.freight_value,
        b._airbyte_emitted_at,
        a.customer_id,
        a.order_status,
        a.order_purchase_timestamp,
        a.order_approved_at,
        a.order_delivered_carrier_date,
        a.order_delivered_customer_date,
        a.order_estimated_delivery_date,
        a.update_at,
        COALESCE(TO_NUMBER(TO_CHAR(a.order_purchase_timestamp::DATE, 'YYYYMMDD')), 19000101) AS purchase_date_id
    FROM {{ ref('orders_silver') }} a
    JOIN {{ ref('order_items_silver') }} b
        ON a.order_id = b.order_id
    JOIN affected_orders ao
        ON ao.order_id = a.order_id
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    _airbyte_emitted_at,
    customer_id,
    order_status,
    order_purchase_timestamp,
    purchase_date_id,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    update_at
FROM joined_data