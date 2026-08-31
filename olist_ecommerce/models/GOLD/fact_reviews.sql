{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='review_id'
) }}

WITH silver_order_reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_creation_date,
        review_answer_timestamp,
        _airbyte_extracted_at,
        update_at
    FROM {{ ref('order_reviews_silver') }}
    {% if is_incremental() %}
        WHERE _airbyte_extracted_at > (SELECT MAX(_airbyte_extracted_at) FROM {{ this }})
    {% endif %}
),

joined_data AS (
    SELECT 
        a.review_id,
        a.order_id,
        a.review_score,
        a.review_creation_date,
        a.review_answer_timestamp,
        a._airbyte_extracted_at,
        a.update_at ,
        b.customer_id
    FROM silver_order_reviews a
    join {{ ref('orders_silver') }} b
        on a.order_id = b.order_id 
)
SELECT 
    review_id,
    order_id,
    customer_id,
    review_creation_date,
    review_score,
    review_answer_timestamp,
    COALESCE(TO_NUMBER(TO_CHAR(review_creation_date::DATE, 'YYYYMMDD')), 19000101) AS review_creation_date_id,    DATEDIFF(hour, review_creation_date, review_answer_timestamp) AS hours_to_answer,
    _airbyte_extracted_at,
    update_at
FROM joined_data