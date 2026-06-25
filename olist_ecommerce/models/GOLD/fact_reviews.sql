{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

WITH silver_order_reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        CAST(review_creation_date AS DATE) AS review_creation_date,
        review_answer_timestamp,
        _airbyte_emitted_at
    FROM {{ ref('order_reviews_silver') }}
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

silver_orders AS (
    SELECT 
        order_id, 
        customer_unique_id 
    FROM {{ ref('orders_silver') }}
),

joined_data AS (
    SELECT 
        a.review_id,
        a.order_id,
        a.review_score,
        a.review_creation_date,
        a.review_answer_timestamp,
        a._airbyte_emitted_at,
        b.customer_unique_id
    FROM silver_order_reviews a
    JOIN silver_orders b ON a.order_id = b.order_id 
)

SELECT 
    review_id,
    order_id,
    customer_unique_id,
    REPLACE(review_creation_date::VARCHAR, '-', '')::INT as review_creation_date_id,
    {{ dbt_utils.generate_surrogate_key(['review_id', 'order_id']) }} AS feedback_id,
    review_score,
    DATEDIFF(hour, review_creation_date, review_answer_timestamp) AS hours_to_answer,
    _airbyte_emitted_at
FROM joined_data;