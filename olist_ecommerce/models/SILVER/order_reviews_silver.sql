{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

WITH bronze_reviews AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _airbyte_emitted_at
    FROM {{ source('BRONZE', 'order_reviews_bronze') }}
    
    {% if is_incremental() %}
        WHERE _airbyte_emitted_at > (SELECT MAX(_airbyte_emitted_at) FROM {{ this }})
    {% endif %}
),

ranked_reviews AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY _airbyte_emitted_at DESC) as rnk
    FROM bronze_reviews
),

deduped_reviews AS (
    SELECT * FROM ranked_reviews WHERE rnk = 1
)

SELECT 
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    _airbyte_emitted_at
FROM deduped_reviews