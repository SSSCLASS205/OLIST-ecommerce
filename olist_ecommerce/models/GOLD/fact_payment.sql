{{ config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential']
) }}

WITH silver_order_payments AS (
    SELECT * FROM {{ ref('order_payments_silver') }}
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

silver_orders AS (
    SELECT 
        order_id,
        customer_unique_id,
        order_status,
        CAST(order_delivered_carrier_date as DATE) as payment_date
    FROM {{ ref('orders_silver') }}
),

joined_data AS (
    SELECT 
        a.order_id,
        a.payment_sequential,
        a.payment_type,
        a.payment_installments,
        a.payment_value,
        a._airbyte_emitted_at,
        b.customer_unique_id,
        b.order_status,
        b.payment_date
    FROM silver_order_payments a
    JOIN silver_orders b ON a.order_id = b.order_id 
)

SELECT 
    order_id,
    payment_sequential,
    customer_unique_id,
    COALESCE(REPLACE(payment_date::VARCHAR, '-', '')::INT, 19000101) as payment_date_id,
    {{ dbt_utils.generate_surrogate_key(['payment_type', 'order_status']) }} AS attribute_id,
    payment_installments,
    payment_value,
    _airbyte_emitted_at
FROM joined_data;