{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['order_id', 'order_item_id']
) }}

with bronze_order_items as (
    SELECT 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        _airbyte_emitted_at
    FROM {{ source('BRONZE', 'order_items_bronze') }}
    
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (select MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

deduped AS (
    SELECT *
    FROM bronze_order_items
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY order_id, order_item_id 
        ORDER BY _airbyte_emitted_at DESC) = 1
)

SELECT 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    _airbyte_emitted_at
FROM deduped