{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='review_id'
    )
}}

WITH silver_order_reviews AS (
    SELECT 
        review_id,
        review_comment_title,
        review_comment_message,
        _airbyte_emitted_at,
        update_at
    FROM {{ ref('order_reviews_silver') }}
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
)

SELECT 
    review_id,
    review_comment_title,
    review_comment_message,
    CASE 
        WHEN review_comment_title IS NOT NULL OR review_comment_message IS NOT NULL 
            THEN TRUE
        ELSE FALSE
    END AS has_comment,
    _airbyte_emitted_at
FROM silver_order_reviews