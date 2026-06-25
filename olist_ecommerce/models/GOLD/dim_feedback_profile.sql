{{ config(
    materialized='incremental',
    unique_key=['review_id','order_id']
) }}

WITH silver_order_reviews AS (
    SELECT 
        review_id,
        order_id,
        review_comment_title,
        review_comment_message,
        _airbyte_emitted_at
    FROM {{ ref('order_reviews_silver') }}
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

silver_orders AS (
    SELECT order_id, customer_unique_id FROM {{ ref('orders_silver') }}
),

joined_data AS (
    SELECT a.*, b.customer_unique_id 
    FROM silver_order_reviews a
    JOIN silver_orders b ON a.order_id = b.order_id 
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['review_id','order_id']) }} AS feedback_id, 
    review_comment_title,
    review_comment_message,
    CASE 
        WHEN review_comment_title IS NOT NULL OR review_comment_message IS NOT NULL 
            THEN TRUE
        ELSE FALSE
    END AS has_comment,
    _airbyte_emitted_at
FROM joined_data