{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    incremental_predicates=[
        "DBT_INTERNAL_DEST.update_at < DBT_INTERNAL_SOURCE.update_at"
    ]
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
        _airbyte_emitted_at,
        update_at
    FROM {{ source('BRONZE', 'orders_bronze') }} 
    
    {% if is_incremental() %}
      WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),
deduped AS (
    SELECT 
        *,
        QUALIFY ROW_NUMBER()
            OVER(PARTITION BY order_id
                    ORDER BY update_at DESC, 
                    _airbyte_emitted_at DESC ) = 1
    FROM bronze_orders
)

SELECT 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    _airbyte_emitted_at,
    update_at
FROM deduped